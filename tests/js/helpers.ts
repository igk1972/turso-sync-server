import { spawn, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach } from 'vitest';
import { connect } from '@tursodatabase/sync';

export const BINARY = process.env.LOCAL_SYNC_SERVER
    ?? new URL('../../target/release/turso-sync-server', import.meta.url).pathname;

const children: ChildProcess[] = [];
const tempDirs: string[] = [];

afterEach(() => {
    for (const child of children.splice(0)) child.kill();
    for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

export function tempDir(): string {
    const dir = mkdtempSync(join(tmpdir(), 'turso-sync-'));
    tempDirs.push(dir);
    return dir;
}

function freePort(): Promise<number> {
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

export async function startServer(args: string[], port?: number, binary = BINARY): Promise<Server> {
    port ??= await freePort();
    // `--sync-server` is understood by both this server and upstream tursodb.
    const child = spawn(binary, [...args, '--sync-server', `127.0.0.1:${port}`], {
        stdio: process.env.SYNC_SERVER_LOGS ? 'inherit' : 'ignore',
    });
    children.push(child);
    const url = `http://127.0.0.1:${port}`;
    const deadline = Date.now() + 10_000;
    while (Date.now() < deadline) {
        if (child.exitCode !== null) throw new Error(`server exited with code ${child.exitCode}`);
        try {
            await fetch(`${url}/health`);
            return { url, port, child };
        } catch { }
        await new Promise(r => setTimeout(r, 50));
    }
    throw new Error('server did not become ready');
}

export async function stopServer(server: Server) {
    if (server.child.exitCode !== null) return;
    const exited = once(server.child, 'exit');
    server.child.kill('SIGTERM');
    await exited;
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

export async function checkpoint(url: string): Promise<{ status: string, generation?: number }> {
    const resp = await fetch(`${url}/admin/checkpoint`, { method: 'POST' });
    if (!resp.ok) throw new Error(`checkpoint failed: ${resp.status} ${await resp.text()}`);
    return resp.json();
}

export async function rows(db: Awaited<ReturnType<typeof connect>>, sql: string) {
    return (await db.prepare(sql)).all();
}
