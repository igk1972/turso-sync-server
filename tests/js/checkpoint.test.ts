import { existsSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { expect, test } from 'vitest';
import { connect } from '@tursodatabase/sync';
import { checkpoint, pipeline, rows, startServer, stopServer, tempDir } from './helpers.js';

type Db = Awaited<ReturnType<typeof connect>>;

const NO_TIMER = ['--checkpoint-interval', '0'];

// Rows are large enough to sit on pages of their own: a lost page then loses
// a row, instead of being masked by a later copy of a shared page.
async function insert(db: Db, ...values: number[]) {
    await db.exec(`INSERT INTO t VALUES ${values.map(v => `(${v}, randomblob(3000))`).join(', ')}`);
    await db.push();
}

async function values(db: Db): Promise<number[]> {
    return (await rows(db, 'SELECT x FROM t ORDER BY x')).map((row: any) => row.x);
}

function range(from: number, to: number): number[] {
    return Array.from({ length: to - from + 1 }, (_, i) => from + i);
}

async function syncedPair(url: string): Promise<{ writer: Db, reader: Db }> {
    const writer = await connect({ path: ':memory:', url });
    await writer.exec('CREATE TABLE t(x INTEGER PRIMARY KEY, y BLOB)');
    await insert(writer, 1);
    const reader = await connect({ path: ':memory:', url });
    expect(await values(reader)).toEqual([1]);
    return { writer, reader };
}

async function waitFor(condition: () => boolean, timeoutMs: number) {
    const deadline = Date.now() + timeoutMs;
    while (!condition()) {
        if (Date.now() > deadline) throw new Error('condition not met in time');
        await new Promise(r => setTimeout(r, 100));
    }
}

test('clients catch up across several checkpoints', async () => {
    const dbPath = join(tempDir(), 'main.db');
    const { url } = await startServer([dbPath, ...NO_TIMER]);
    const { writer, reader } = await syncedPair(url);

    for (const x of [2, 3, 4]) {
        await insert(writer, x);
        expect(await checkpoint(url)).toMatchObject({ status: 'done', generation: x - 1 });
    }
    expect(statSync(`${dbPath}-wal`).size).toBe(0);
    expect(existsSync(join(`${dbPath}-sync`, 'generations', '0.pages'))).toBe(true);
    expect(await checkpoint(url)).toEqual({ status: 'skipped' });

    await reader.pull();
    expect(await values(reader)).toEqual([1, 2, 3, 4]);
    const fresh = await connect({ path: ':memory:', url });
    expect(await values(fresh)).toEqual([1, 2, 3, 4]);
});

test('clients keep syncing when the server restarts after a checkpoint', async () => {
    const dbPath = join(tempDir(), 'main.db');
    let server = await startServer([dbPath, ...NO_TIMER]);
    const { writer, reader } = await syncedPair(server.url);

    await insert(writer, 2);
    expect(await checkpoint(server.url)).toMatchObject({ status: 'done' });
    await insert(writer, 3);

    await stopServer(server);
    server = await startServer([dbPath, ...NO_TIMER], server.port);

    await insert(writer, 4);
    await reader.pull();
    expect(await values(reader)).toEqual([1, 2, 3, 4]);
});

test('a WAL reset by a request makes older clients download everything', async () => {
    const dbPath = join(tempDir(), 'main.db');
    const { url } = await startServer([dbPath]);
    const { writer, reader } = await syncedPair(url);

    await insert(writer, 2);
    expect((await pipeline(url, ['PRAGMA wal_checkpoint(TRUNCATE)'])).status).toBe(200);
    // The restarted WAL has to outgrow the reader's frame, or the reader's
    // revision would look like it comes from the future.
    for (const x of range(3, 12)) await insert(writer, x);

    await reader.pull();
    expect(await values(reader)).toEqual(range(1, 12));
});

test('changes made while the server is down make older clients download everything', async () => {
    const dbPath = join(tempDir(), 'main.db');
    let server = await startServer([dbPath]);
    const writer = await connect({ path: ':memory:', url: server.url });
    await writer.exec('CREATE TABLE t(x INTEGER PRIMARY KEY, y BLOB)');
    await writer.exec('CREATE TABLE offline(x INTEGER PRIMARY KEY, y BLOB)');
    await insert(writer, 1);
    const reader = await connect({ path: ':memory:', url: server.url });
    await stopServer(server);

    // A table nobody touches afterwards: its page is only in the database file.
    // SQLite folds the WAL into the database file and deletes it on close.
    const sqlite = new DatabaseSync(dbPath);
    sqlite.exec('INSERT INTO offline VALUES (100, randomblob(3000))');
    sqlite.close();
    expect(existsSync(`${dbPath}-wal`)).toBe(false);

    server = await startServer([dbPath], server.port);
    for (const x of range(2, 12)) await insert(writer, x);

    await reader.pull();
    expect(await values(reader)).toEqual(range(1, 12));
    expect(await rows(reader, 'SELECT x FROM offline')).toEqual([{ x: 100 }]);
});

test('a chunked bootstrap holds checkpoints off', async () => {
    const dbPath = join(tempDir(), 'main.db');
    const { url } = await startServer([dbPath, ...NO_TIMER]);

    const seed = await connect({ path: ':memory:', url });
    await seed.exec('CREATE TABLE big(x INTEGER PRIMARY KEY, y BLOB)');
    await seed.exec('INSERT INTO big SELECT value, randomblob(1024) FROM generate_series(1, 50)');
    await seed.push();
    expect(await checkpoint(url)).toMatchObject({ status: 'done' });
    await seed.exec('INSERT INTO big SELECT value, randomblob(1024) FROM generate_series(51, 60)');
    await seed.push();

    const chunked = await connect({ path: ':memory:', url, pullBytesThreshold: 8192 });
    expect(await rows(chunked, 'SELECT COUNT(*) AS c, SUM(length(y)) AS bytes FROM big'))
        .toEqual([{ c: 60, bytes: 60 * 1024 }]);
    expect(await checkpoint(url)).toEqual({ status: 'deferred' });
});

test('periodic checkpoints run on their own', async () => {
    const dbPath = join(tempDir(), 'main.db');
    const { url } = await startServer([
        dbPath, '--checkpoint-interval', '1', '--checkpoint-min-frames', '1',
    ]);
    const { writer, reader } = await syncedPair(url);
    await insert(writer, 2);

    await waitFor(() => existsSync(join(`${dbPath}-sync`, 'generations', '0.pages')), 5000);
    await waitFor(() => statSync(`${dbPath}-wal`).size === 0, 5000);
    await reader.pull();
    expect(await values(reader)).toEqual([1, 2]);
});

test.skipIf(!process.env.UPSTREAM_TURSODB)('clients of tursodb keep syncing after switching servers', async () => {
    const dbPath = join(tempDir(), 'main.db');
    let server = await startServer([dbPath], undefined, process.env.UPSTREAM_TURSODB);
    const { writer, reader } = await syncedPair(server.url);
    await insert(writer, 2);
    await stopServer(server);

    server = await startServer([dbPath], server.port);
    for (const x of range(3, 12)) await insert(writer, x);

    await reader.pull();
    expect(await values(reader)).toEqual(range(1, 12));
    const fresh = await connect({ path: ':memory:', url: server.url });
    expect(await values(fresh)).toEqual(range(1, 12));
});

test('databases under --sync-dir are checkpointed one by one', async () => {
    const dir = tempDir();
    const { url } = await startServer(['--sync-dir', dir, ...NO_TIMER]);
    const alpha = await syncedPair(`${url}/db/alpha`);
    await syncedPair(`${url}/db/beta`);

    await insert(alpha.writer, 2);
    const resp = await fetch(`${url}/db/alpha/admin/checkpoint`, { method: 'POST' });
    expect(await resp.json()).toMatchObject({ status: 'done', generation: 1 });
    expect(existsSync(join(dir, 'alpha', 'data-sync', 'generations', '0.pages'))).toBe(true);
    expect(existsSync(join(dir, 'beta', 'data-sync', 'generations', '0.pages'))).toBe(false);

    await alpha.reader.pull();
    expect(await values(alpha.reader)).toEqual([1, 2]);
});
