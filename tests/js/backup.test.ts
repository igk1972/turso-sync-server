import { randomUUID } from 'node:crypto';
import { mkdirSync, readdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { expect, test } from 'vitest';
import { connect } from '@tursodatabase/sync';
import {
    NO_TIMER, backedUpFrames, checkpoint, cli, generations, insert, pipeline, range, removeDatabase,
    sqliteValues, startServer, stopServer, syncedPair, tempDir, values, waitFor, walFrames,
} from './helpers.js';

// S3-compatible storage to run the restore tests against as well, e.g.
// s3://bucket/prefix, configured through the AWS_* variables.
const S3_URL = process.env.BACKUP_S3_URL;

const targets = [
    { name: 'a directory', url: (dir: string) => join(dir, 'backup') },
    ...(S3_URL ? [{ name: 'S3', url: () => `${S3_URL}/${randomUUID()}` }] : []),
];

for (const target of targets) {
    test(`a database comes back from a backup in ${target.name}`, async () => {
        const dir = tempDir();
        const dbPath = join(dir, 'main.db');
        const backupUrl = target.url(dir);
        const args = [dbPath, ...NO_TIMER, '--backup-url', backupUrl];
        let server = await startServer(args);
        const { writer, reader } = await syncedPair(server.url);
        for (const x of [2, 3]) {
            await insert(writer, x);
            expect(await checkpoint(server.url)).toMatchObject({ status: 'done' });
        }
        await insert(writer, 4);
        // Stopping uploads the WAL written since the last backup tick.
        await stopServer(server);
        expect(sqliteValues(dbPath)).toEqual([1, 2, 3, 4]);
        removeDatabase(dbPath);

        server = await startServer(args, server.port);
        const fresh = await connect({ path: ':memory:', url: server.url });
        expect(await values(fresh)).toEqual([1, 2, 3, 4]);
        await reader.pull();
        expect(await values(reader)).toEqual([1, 2, 3, 4]);

        // The restored database keeps extending the same backup history.
        await insert(writer, 5);
        expect(await checkpoint(server.url)).toMatchObject({ status: 'done' });
        await insert(writer, 6);
        await stopServer(server);
        removeDatabase(dbPath);
        server = await startServer(args, server.port);
        await reader.pull();
        expect(await values(reader)).toEqual(range(1, 6));
        await stopServer(server);

        const history = generations(backupUrl);
        expect(history.filter(g => g.base === 'snapshot')).toHaveLength(1);
        expect(history.every(g => g.base !== 'incomplete')).toBe(true);
    });
}

test('writes reach the backup within a second, so a crash loses at most that', async () => {
    const dir = tempDir();
    const dbPath = join(dir, 'main.db');
    const backupDir = join(dir, 'backup');
    const args = [dbPath, ...NO_TIMER, '--backup-url', backupDir];
    let server = await startServer(args);
    const { writer } = await syncedPair(server.url);
    await insert(writer, 2, 3);
    await waitFor(() => backedUpFrames(backupDir) >= walFrames(dbPath), 5000);

    server.child.kill('SIGKILL');
    await new Promise(r => server.child.once('exit', r));
    removeDatabase(dbPath);

    server = await startServer(args, server.port);
    const fresh = await connect({ path: ':memory:', url: server.url });
    expect(await values(fresh)).toEqual([1, 2, 3]);
});

test('restore --at returns the database as it was at that time', async () => {
    const dir = tempDir();
    const dbPath = join(dir, 'main.db');
    const backupDir = join(dir, 'backup');
    const server = await startServer([dbPath, ...NO_TIMER, '--backup-url', backupDir]);
    const { writer } = await syncedPair(server.url);
    await waitFor(() => backedUpFrames(backupDir) >= walFrames(dbPath), 5000);
    const before = new Date();
    await new Promise(r => setTimeout(r, 50));
    await insert(writer, 2);
    await stopServer(server);

    const past = join(dir, 'past.db');
    expect(cli('restore', '--backup-url', backupDir, '--at', before.toISOString(), past))
        .toMatch(/^restored /);
    expect(sqliteValues(past)).toEqual([1]);
    const latest = join(dir, 'latest.db');
    cli('restore', '--backup-url', backupDir, latest);
    expect(sqliteValues(latest)).toEqual([1, 2]);

    expect(() => cli('restore', '--backup-url', backupDir, latest)).toThrow(/--force/);
    expect(() => cli('restore', '--backup-url', join(dir, 'nothing'), join(dir, 'none.db')))
        .toThrow(/holds no backup/);
});

test('a finished generation is merged into one object that restores alone', async () => {
    const dir = tempDir();
    const dbPath = join(dir, 'main.db');
    const backupDir = join(dir, 'backup');
    const server = await startServer([dbPath, ...NO_TIMER, '--backup-url', backupDir]);
    const { writer } = await syncedPair(server.url);
    let middle = new Date();
    for (const x of range(2, 5)) {
        await insert(writer, x);
        await waitFor(() => backedUpFrames(backupDir) >= walFrames(dbPath), 5000);
        if (x === 3) middle = new Date();
    }
    const [first] = generations(backupDir);
    const framesDir = join(backupDir, 'generations', first.id, 'frames');
    const uploads = readdirSync(framesDir);
    expect(uploads.length).toBeGreaterThan(1);
    expect(await checkpoint(server.url)).toMatchObject({ status: 'done' });
    const merged = `${String(1).padStart(12, '0')}-${String(first.frames).padStart(12, '0')}.zst`;
    await waitFor(() => readdirSync(framesDir).includes(merged), 5000);
    await stopServer(server);

    const past = join(dir, 'past.db');
    cli('restore', '--backup-url', backupDir, '--at', middle.toISOString(), past);
    expect(sqliteValues(past)).toEqual([1, 2, 3]);
    for (const name of uploads) rmSync(join(framesDir, name));
    const latest = join(dir, 'latest.db');
    cli('restore', '--backup-url', backupDir, latest);
    expect(sqliteValues(latest)).toEqual(range(1, 5));
});

test('a WAL reset by a request makes the backup start over from a snapshot', async () => {
    const dir = tempDir();
    const dbPath = join(dir, 'main.db');
    const backupDir = join(dir, 'backup');
    const server = await startServer([dbPath, '--backup-url', backupDir]);
    const { writer } = await syncedPair(server.url);
    await insert(writer, 2);
    expect((await pipeline(server.url, ['PRAGMA wal_checkpoint(TRUNCATE)'])).status).toBe(200);
    for (const x of range(3, 5)) await insert(writer, x);
    await stopServer(server);

    expect(generations(backupDir).filter(g => g.base === 'snapshot')).toHaveLength(2);
    const restored = join(dir, 'restored.db');
    cli('restore', '--backup-url', backupDir, restored);
    expect(sqliteValues(restored)).toEqual(range(1, 5));
});

test('databases under --sync-dir are backed up and restored one by one', async () => {
    const dir = tempDir();
    const data = join(dir, 'data');
    mkdirSync(data);
    const backupDir = join(dir, 'backup');
    const args = ['--sync-dir', data, ...NO_TIMER, '--backup-url', backupDir];
    let server = await startServer(args);
    const alpha = await syncedPair(`${server.url}/db/alpha`);
    await insert(alpha.writer, 2);
    const beta = await syncedPair(`${server.url}/db/beta`);
    await stopServer(server);

    const restored = join(dir, 'beta.db');
    cli('restore', '--backup-url', join(backupDir, 'beta'), restored);
    expect(sqliteValues(restored)).toEqual([1]);

    rmSync(join(data, 'alpha'), { recursive: true });
    server = await startServer(args, server.port);
    const fresh = await connect({ path: ':memory:', url: `${server.url}/db/alpha` });
    expect(await values(fresh)).toEqual([1, 2]);
    await beta.reader.pull();
    expect(await values(beta.reader)).toEqual([1]);
});
