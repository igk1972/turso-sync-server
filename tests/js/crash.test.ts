import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { expect, test } from 'vitest';
import { connect } from '@tursodatabase/sync';
import {
    BINARY, NO_TIMER, type Db, type Server, checkpoint, cli, freePort, generations, insert, range,
    removeDatabase, rows, sql, sqliteValues, startServer, stopServer, syncedPair, tempDir, values,
    waitFor,
} from './helpers.js';

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

/** Runs the server until it kills itself at a crash point. */
async function runUntilCrash(args: string[], point: string) {
    const child = spawn(BINARY, [...args, '--sync-server', '127.0.0.1:0'], {
        stdio: process.env.SYNC_SERVER_LOGS ? 'inherit' : 'ignore',
        env: { ...process.env, TURSO_SYNC_CRASH_AT: point },
    });
    const timer = setTimeout(() => child.kill(), 10_000);
    const [, signal] = await once(child, 'exit');
    clearTimeout(timer);
    expect(signal).toBe('SIGKILL');
}

async function killedBy(server: Server, action: () => Promise<unknown>) {
    const exit = once(server.child, 'exit');
    await expect(action()).rejects.toThrow();
    expect((await exit)[1]).toBe('SIGKILL');
}

async function kill(server: Server) {
    const exit = once(server.child, 'exit');
    server.child.kill('SIGKILL');
    await exit;
}

function restoredValues(backupUrl: string, dir: string): number[] {
    const restored = join(dir, 'restored.db');
    cli('restore', '--backup-url', backupUrl, restored);
    try {
        return sqliteValues(restored);
    } finally {
        removeDatabase(restored);
    }
}

/** Run after a graceful stop: the backup restores the database file byte for byte. */
function expectBackupMatches(dbPath: string, backupUrl: string) {
    const restored = `${dbPath}.restored`;
    cli('restore', '--backup-url', backupUrl, restored);
    // Reading through SQLite and closing folds the WAL into the database file.
    const db = new DatabaseSync(dbPath);
    db.prepare('SELECT count(*) FROM sqlite_schema').get();
    db.close();
    expect(readFileSync(restored).equals(readFileSync(dbPath))).toBe(true);
}

/** Waits for the snapshot uploaded at startup, so that later uploads are predictable. */
async function backupStarted(backupUrl: string) {
    await waitFor(() => generations(backupUrl).some(g => g.base === 'snapshot'), 10_000);
}

function databaseFiles(dir: string) {
    const dbPath = join(dir, 'main.db');
    const backupUrl = join(dir, 'backup');
    return { dbPath, backupUrl, args: [dbPath, ...NO_TIMER, '--backup-url', backupUrl] };
}

for (const point of ['checkpoint.sealed', 'checkpoint.truncated', 'checkpoint.identity-stored']) {
    test(`a crash at ${point} loses no changes`, async () => {
        const dir = tempDir();
        const { dbPath, backupUrl, args } = databaseFiles(dir);
        let server = await startServer(args, undefined, { env: { TURSO_SYNC_CRASH_AT: point } });
        const { writer, reader: early } = await syncedPair(server.url);
        await insert(writer, 2);
        const late = await connect({ path: ':memory:', url: server.url });
        await killedBy(server, () => checkpoint(server.url));
        // A checkpoint starts by uploading the WAL.
        expect(restoredValues(backupUrl, dir)).toEqual([1, 2]);

        server = await startServer(args, server.port);
        await early.pull();
        expect(await values(early)).toEqual([1, 2]);
        // Enough writes for the WAL to grow past the frame the late reader stands at.
        for (const x of range(3, 8)) await insert(writer, x);
        await late.pull();
        expect(await values(late)).toEqual(range(1, 8));
        expect(await checkpoint(server.url)).toMatchObject({ status: 'done' });
        await insert(writer, 9);
        for (const reader of [early, late]) {
            await reader.pull();
            expect(await values(reader)).toEqual(range(1, 9));
        }
        expect(await values(await connect({ path: ':memory:', url: server.url }))).toEqual(range(1, 9));
        await stopServer(server);
        expectBackupMatches(dbPath, backupUrl);
    });
}

// The backup interval keeps uploads to startup, checkpoints and shutdown.
const RARE_UPLOADS = ['--backup-interval', '3600'];

test('a WAL upload interrupted before it is recorded is repeated', async () => {
    const dir = tempDir();
    const { dbPath, backupUrl } = databaseFiles(dir);
    const args = [...databaseFiles(dir).args, ...RARE_UPLOADS];
    let server = await startServer(args);
    await backupStarted(backupUrl);
    const { writer, reader } = await syncedPair(server.url);
    await insert(writer, 2);
    await kill(server);
    await runUntilCrash(args, 'backup.frames-uploaded');
    expect(restoredValues(backupUrl, dir)).toEqual([1, 2]);

    server = await startServer(args, server.port);
    await insert(writer, 3);
    await reader.pull();
    expect(await values(reader)).toEqual(range(1, 3));
    await stopServer(server);
    expectBackupMatches(dbPath, backupUrl);
});

test('a snapshot upload interrupted before it is recorded is repeated', async () => {
    const dir = tempDir();
    const { dbPath, backupUrl } = databaseFiles(dir);
    const args = [...databaseFiles(dir).args, ...RARE_UPLOADS, '--backup-snapshot-interval', '0'];
    let server = await startServer(args);
    await backupStarted(backupUrl);
    const { writer, reader } = await syncedPair(server.url);
    // The backup generation after this checkpoint starts from a snapshot.
    expect(await checkpoint(server.url)).toMatchObject({ status: 'done' });
    await insert(writer, 2);
    await kill(server);
    await runUntilCrash(args, 'backup.snapshot-uploaded');
    // The new generation is not restorable yet; the previous one ends at the checkpoint.
    expect(restoredValues(backupUrl, dir)).toEqual([1]);

    server = await startServer(args, server.port);
    await insert(writer, 3);
    await reader.pull();
    expect(await values(reader)).toEqual(range(1, 3));
    await stopServer(server);
    expectBackupMatches(dbPath, backupUrl);
    expect(generations(backupUrl).at(-1)!.base).toBe('snapshot');
});

for (const point of ['restore.downloaded', 'restore.installed']) {
    test(`a restore interrupted at ${point} completes on the next start`, async () => {
        const { dbPath, backupUrl, args } = databaseFiles(tempDir());
        let server = await startServer(args);
        const { writer, reader } = await syncedPair(server.url);
        await insert(writer, 2);
        expect(await checkpoint(server.url)).toMatchObject({ status: 'done' });
        await insert(writer, 3);
        await stopServer(server);
        removeDatabase(dbPath);

        await runUntilCrash(args, point);
        server = await startServer(args, server.port);
        expect(await values(await connect({ path: ':memory:', url: server.url }))).toEqual(range(1, 3));
        await insert(writer, 4);
        await reader.pull();
        expect(await values(reader)).toEqual(range(1, 4));
        await stopServer(server);
        expectBackupMatches(dbPath, backupUrl);
    });
}

const CRASHES = Number(process.env.CRASH_ITERATIONS ?? 10);
/** A write acknowledged this long before a crash has to be in the backup. */
const BACKUP_LAG_MS = 2000;
const WRITERS = 3;
const ID_SPAN = 1_000_000;

/** Writer `w` inserts `w * ID_SPAN + 1`, `+ 2`, … */
function rowsPerWriter(xs: number[]): number[] {
    const counts = Array(WRITERS + 1).fill(0);
    for (const x of xs) {
        const w = Math.floor(x / ID_SPAN);
        counts[w]++;
        // Writers push in order, so any consistent state holds a prefix of each writer's rows.
        expect(x).toBe(w * ID_SPAN + counts[w]);
    }
    return counts;
}

async function digest(db: Db): Promise<string> {
    const hash = createHash('sha256');
    for (const row of await rows(db, 'SELECT x, hex(y) AS y FROM t ORDER BY x')) {
        hash.update(`${row.x}:${row.y};`);
    }
    return hash.digest('hex');
}

test(`acknowledged writes survive ${CRASHES} kills at random moments`, async () => {
    const dir = tempDir();
    const { dbPath, backupUrl } = databaseFiles(dir);
    const port = await freePort();
    const modes = [[], ['--checkpoint-after-write'], ['--backup-snapshot-interval', '0']];
    const args = (i: number) => [...databaseFiles(dir).args, ...modes[i % modes.length]];

    let server = await startServer(args(0), port);
    // More than one streamed batch of pages, so that bootstraps are streamed.
    await sql(server.url, 'CREATE TABLE pad(y BLOB)');
    await sql(server.url, 'INSERT INTO pad SELECT randomblob(4000) FROM generate_series(1, 300)');
    await sql(server.url, 'CREATE TABLE t(x INTEGER PRIMARY KEY, y BLOB)');
    const clients = () => Promise.all(range(1, WRITERS).map(() => connect({ path: ':memory:', url: server.url })));
    const writers = await clients();
    const readers = await clients();
    const attempted = Array(WRITERS + 1).fill(0);
    const acked = Array(WRITERS + 1).fill(0);
    const ackedAt: number[][] = range(0, WRITERS).map(() => []);
    let worstLag = 0;

    for (let i = 0; i < CRASHES; i++) {
        let killedAt = Infinity;
        const failures: string[] = [];
        const failed = (what: string, err: unknown) => {
            if (Date.now() < killedAt) failures.push(`${what}: ${err}`);
        };
        const loops = [
            ...writers.map(async (db, index) => {
                const w = index + 1;
                for (;;) {
                    const k = attempted[w] + 1;
                    await db.exec(`INSERT INTO t VALUES (${w * ID_SPAN + k}, randomblob(500))`);
                    attempted[w] = k;
                    try {
                        await db.push();
                    } catch (err) {
                        return failed(`writer ${w}`, err);
                    }
                    const now = Date.now();
                    for (let n = acked[w] + 1; n <= k; n++) ackedAt[w][n] = now;
                    acked[w] = k;
                }
            }),
            ...readers.map(async (db, index) => {
                for (;;) {
                    try {
                        await db.pull();
                    } catch (err) {
                        return failed(`reader ${index}`, err);
                    }
                    await sleep(5);
                }
            }),
            (async () => {
                for (;;) {
                    await sleep(Math.random() * 200);
                    const floor = [...acked];
                    let db: Db;
                    try {
                        db = await connect({ path: ':memory:', url: server.url });
                    } catch (err) {
                        return failed('bootstrap', err);
                    }
                    const held = rowsPerWriter(await values(db));
                    for (let w = 1; w <= WRITERS; w++) expect(held[w]).toBeGreaterThanOrEqual(floor[w]);
                    expect(await rows(db, 'SELECT count(*) AS c FROM pad')).toEqual([{ c: 300 }]);
                    await db.close();
                }
            })(),
            (async () => {
                for (;;) {
                    await sleep(Math.random() * 100);
                    try {
                        await checkpoint(server.url);
                    } catch (err) {
                        return failed('checkpoint', err);
                    }
                }
            })(),
        ];

        await sleep(100 + Math.random() * 1400);
        killedAt = Date.now();
        await kill(server);
        await Promise.all(loops);
        expect(failures).toEqual([]);

        const backedUp = rowsPerWriter(restoredValues(backupUrl, dir));
        for (let w = 1; w <= WRITERS; w++) {
            expect(backedUp[w]).toBeLessThanOrEqual(attempted[w]);
            if (backedUp[w] < acked[w]) {
                worstLag = Math.max(worstLag, killedAt - ackedAt[w][backedUp[w] + 1]);
            }
        }
        expect(worstLag).toBeLessThan(BACKUP_LAG_MS);

        server = await startServer(args(i + 1), port);
        const fresh = await connect({ path: ':memory:', url: server.url });
        const held = rowsPerWriter(await values(fresh));
        await fresh.close();
        for (let w = 1; w <= WRITERS; w++) {
            expect(held[w]).toBeGreaterThanOrEqual(acked[w]);
            expect(held[w]).toBeLessThanOrEqual(attempted[w]);
        }
        // Writes whose push the crash cut short go through now, exactly once.
        for (const db of writers) await db.push();
        const now = Date.now();
        for (let w = 1; w <= WRITERS; w++) {
            for (let n = acked[w] + 1; n <= attempted[w]; n++) ackedAt[w][n] = now;
            acked[w] = attempted[w];
        }
        const all = await connect({ path: ':memory:', url: server.url });
        expect(rowsPerWriter(await values(all))).toEqual(attempted);
        const expected = await digest(all);
        await all.close();
        for (const db of [...writers, ...readers]) {
            await db.pull();
            expect(await digest(db)).toBe(expected);
        }
    }
    await stopServer(server);
    expectBackupMatches(dbPath, backupUrl);
    console.log(`${CRASHES} crashes, ${acked.reduce((a, b) => a + b)} rows, worst backup lag ${worstLag} ms`);
}, CRASHES * 20_000 + 30_000);
