import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import { existsSync, mkdtempSync, readdirSync, rmSync, statSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { afterEach, expect } from 'vitest';
import { connect } from '@tursodatabase/sync';

export const BINARY = process.env.LOCAL_SYNC_SERVER
    ?? new URL('../../target/release/turso-sync-server', import.meta.url).pathname;

const children: ChildProcess[] = [];
const tempDirs: string[] = [];

afterEach(async () => {
    // A server still running would keep writing into the directories removed below.
    await Promise.all(children.splice(0).filter(child => !exited(child)).map(child => {
        const done = once(child, 'exit');
        child.kill('SIGKILL');
        return done;
    }));
    for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

export function tempDir(): string {
    const dir = mkdtempSync(join(tmpdir(), 'turso-sync-'));
    tempDirs.push(dir);
    return dir;
}

export function freePort(): Promise<number> {
    return new Promise((resolve, reject) => {
        const server = createServer();
        server.unref();
        server.on('error', reject);
        server.listen(0, '127.0.0.1', () => {
            const port = (server.address() as { port: number }).port;
            server.close(() => resolve(port));
        });
    });
}

export interface Server {
    url: string;
    port: number;
    child: ChildProcess;
}

export async function startServer(
    args: string[],
    port?: number,
    { binary = BINARY, env = {} }: { binary?: string, env?: Record<string, string> } = {},
): Promise<Server> {
    port ??= await freePort();
    // `--sync-server` is understood by both this server and upstream tursodb.
    const child = spawn(binary, [...args, '--sync-server', `127.0.0.1:${port}`], {
        stdio: process.env.SYNC_SERVER_LOGS ? 'inherit' : 'ignore',
        env: { ...process.env, ...env },
    });
    children.push(child);
    const url = `http://127.0.0.1:${port}`;
    const deadline = Date.now() + 10_000;
    while (Date.now() < deadline) {
        if (exited(child)) throw new Error(`server exited: ${child.exitCode ?? child.signalCode}`);
        try {
            await fetch(`${url}/health`);
            return { url, port, child };
        } catch { }
        await new Promise(r => setTimeout(r, 50));
    }
    throw new Error('server did not become ready');
}

export function exited(child: ChildProcess): boolean {
    return child.exitCode !== null || child.signalCode !== null;
}

export async function stopServer(server: Server) {
    if (exited(server.child)) return;
    const done = once(server.child, 'exit');
    server.child.kill('SIGTERM');
    await done;
}

export function pipeline(url: string, statements: string[], token?: string): Promise<Response> {
    return fetch(`${url}/v2/pipeline`, {
        method: 'POST',
        headers: {
            'content-type': 'application/json',
            ...(token ? { authorization: `Bearer ${token}` } : {}),
        },
        body: JSON.stringify({
            requests: statements.map(sql => ({ type: 'execute', stmt: { sql } })),
        }),
    });
}

export async function sql(url: string, statement: string): Promise<any[][]> {
    const resp = await pipeline(url, [statement]);
    expect(resp.status).toBe(200);
    const result = (await resp.json()).results[0];
    if (result.type !== 'ok') throw new Error(`${statement}: ${JSON.stringify(result)}`);
    return result.response.result.rows;
}

export async function checkpoint(url: string): Promise<{ status: string, generation?: number }> {
    const resp = await fetch(`${url}/admin/checkpoint`, { method: 'POST' });
    if (!resp.ok) throw new Error(`checkpoint failed: ${resp.status} ${await resp.text()}`);
    return resp.json();
}

export type Db = Awaited<ReturnType<typeof connect>>;

export const NO_TIMER = ['--checkpoint-interval', '0'];

export async function rows(db: Db, sql: string) {
    return (await db.prepare(sql)).all();
}

// Rows are large enough to sit on pages of their own: a lost page then loses
// a row, instead of being masked by a later copy of a shared page.
export async function insert(db: Db, ...values: number[]) {
    await db.exec(`INSERT INTO t VALUES ${values.map(v => `(${v}, randomblob(3000))`).join(', ')}`);
    await db.push();
}

export async function values(db: Db): Promise<number[]> {
    return (await rows(db, 'SELECT x FROM t ORDER BY x')).map((row: any) => row.x);
}

export function range(from: number, to: number): number[] {
    return Array.from({ length: to - from + 1 }, (_, i) => from + i);
}

export async function syncedPair(url: string): Promise<{ writer: Db, reader: Db }> {
    const writer = await connect({ path: ':memory:', url });
    await writer.exec('CREATE TABLE t(x INTEGER PRIMARY KEY, y BLOB)');
    await insert(writer, 1);
    const reader = await connect({ path: ':memory:', url });
    expect(await values(reader)).toEqual([1]);
    return { writer, reader };
}

export async function waitFor(condition: () => boolean, timeoutMs: number) {
    const deadline = Date.now() + timeoutMs;
    while (!condition()) {
        if (Date.now() > deadline) throw new Error('condition not met in time');
        await new Promise(r => setTimeout(r, 100));
    }
}

const WAL_HEADER_SIZE = 32;
const WAL_FRAME_SIZE = 24 + 4096;

export function cli(...args: string[]): string {
    return execFileSync(BINARY, args, { encoding: 'utf8' });
}

export function removeDatabase(dbPath: string) {
    for (const suffix of ['', '-wal', '-shm']) rmSync(`${dbPath}${suffix}`, { force: true });
    rmSync(`${dbPath}-sync`, { recursive: true, force: true });
}

export function sqliteValues(dbPath: string): number[] {
    const db = new DatabaseSync(dbPath);
    try {
        expect(db.prepare('PRAGMA integrity_check').get()).toEqual({ integrity_check: 'ok' });
        return db.prepare('SELECT x FROM t ORDER BY x').all().map((row: any) => row.x);
    } finally {
        db.close();
    }
}

export function walFrames(dbPath: string): number {
    const wal = `${dbPath}-wal`;
    return existsSync(wal) ? Math.max(0, (statSync(wal).size - WAL_HEADER_SIZE) / WAL_FRAME_SIZE) : 0;
}

/** The last frame uploaded for the newest generation of a directory backup. */
export function backedUpFrames(backupDir: string): number {
    const root = join(backupDir, 'generations');
    if (!existsSync(root)) return 0;
    const newest = readdirSync(root).sort().at(-1);
    const frames = newest ? join(root, newest, 'frames') : '';
    if (!newest || !existsSync(frames)) return 0;
    // Objects being written have a temporary name with a suffix after `.zst`.
    const uploaded = readdirSync(frames).filter(name => name.endsWith('.zst'));
    return Math.max(0, ...uploaded.map(name => Number(name.split(/[-.]/)[1])));
}

/** Generations listed by the `backups` command, oldest first. */
export function generations(backupUrl: string): { id: string, frames: number, base: string }[] {
    return cli('backups', '--backup-url', backupUrl)
        .trim()
        .split('\n')
        .slice(1)
        .map(line => {
            const [id, , , frames, ...base] = line.split(/\s+/);
            return { id, frames: Number(frames), base: base.join(' ') };
        });
}
