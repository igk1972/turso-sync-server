import { once } from 'node:events';
import { existsSync } from 'node:fs';
import { connect as netConnect, type Socket } from 'node:net';
import { join } from 'node:path';
import { expect, test } from 'vitest';
import { connect } from '@tursodatabase/sync';
import { freePort, pipeline, rows, startServer, stopServer, tempDir } from './helpers.js';

async function openStalledRequest(port: number): Promise<Socket> {
    const socket = netConnect(port, '127.0.0.1');
    await once(socket, 'connect');
    socket.write('POST /v2/pipeline HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n{"requ');
    return socket;
}

test('sync endpoints require the auth token', async () => {
    const { url } = await startServer([':memory:', '--auth-token', 'secret']);

    expect((await pipeline(url, ['SELECT 1'])).status).toBe(401);
    expect((await pipeline(url, ['SELECT 1'], 'wrong')).status).toBe(401);
    expect((await pipeline(url, ['SELECT 1'], 'secret')).status).toBe(200);
    expect((await fetch(`${url}/pull-updates`, { method: 'POST' })).status).toBe(401);
    expect((await fetch(`${url}/health`)).status).toBe(200);

    const preflight = await fetch(`${url}/v2/pipeline`, { method: 'OPTIONS' });
    expect(preflight.status).toBe(204);
    expect(preflight.headers.get('access-control-allow-headers')).toContain('Authorization');
});

test('clients sync through an authenticated server', async () => {
    const { url } = await startServer([':memory:', '--auth-token', 'secret']);

    const writer = await connect({ path: ':memory:', url, authToken: 'secret' });
    await writer.exec('CREATE TABLE t(x)');
    await writer.exec('INSERT INTO t VALUES (1), (2)');
    await writer.push();

    const reader = await connect({ path: ':memory:', url, authToken: 'secret' });
    expect(await rows(reader, 'SELECT x FROM t ORDER BY x')).toEqual([{ x: 1 }, { x: 2 }]);

    await expect(connect({ path: ':memory:', url })).rejects.toThrow();
    await expect(connect({ path: ':memory:', url, authToken: 'wrong' })).rejects.toThrow();
});

test('a stalled connection does not block other clients', async () => {
    const { url, port } = await startServer([':memory:']);
    const stalled = await openStalledRequest(port);
    try {
        const started = Date.now();
        const db = await connect({ path: ':memory:', url });
        await db.exec('CREATE TABLE t(x)');
        await db.exec('INSERT INTO t VALUES (1)');
        await db.push();
        await db.pull();
        expect(Date.now() - started).toBeLessThan(3000);
    } finally {
        stalled.destroy();
    }
});

test('a stalled request is cut off by the request timeout', async () => {
    const { port } = await startServer([':memory:', '--request-timeout', '1']);
    const stalled = await openStalledRequest(port);
    stalled.setEncoding('utf8');
    const [chunk] = await once(stalled, 'data');
    expect(chunk).toMatch(/^HTTP\/1\.1 408/);
    stalled.destroy();
});

test('a transaction left open by one request does not capture later writes', async () => {
    const { url } = await startServer([':memory:']);

    expect((await pipeline(url, ['CREATE TABLE t(x)'])).status).toBe(200);
    expect((await pipeline(url, ['BEGIN', 'INSERT INTO t VALUES (1)'])).status).toBe(200);
    expect((await pipeline(url, ['INSERT INTO t VALUES (2)'])).status).toBe(200);

    const db = await connect({ path: ':memory:', url });
    expect(await rows(db, 'SELECT x FROM t')).toEqual([{ x: 2 }]);
});

test('--sync-dir serves independent databases', async () => {
    const dir = tempDir();
    const { url } = await startServer(['--sync-dir', dir]);

    for (const name of ['alpha', 'beta']) {
        const db = await connect({ path: ':memory:', url: `${url}/db/${name}` });
        await db.exec('CREATE TABLE t(x)');
        await db.exec(`INSERT INTO t VALUES ('${name}')`);
        await db.push();
    }

    const alpha = await connect({ path: ':memory:', url: `${url}/db/alpha` });
    expect(await rows(alpha, 'SELECT x FROM t')).toEqual([{ x: 'alpha' }]);
    const beta = await connect({ path: ':memory:', url: `${url}/db/beta` });
    expect(await rows(beta, 'SELECT x FROM t')).toEqual([{ x: 'beta' }]);

    expect(existsSync(join(dir, 'alpha', 'data'))).toBe(true);
    expect((await pipeline(url, ['SELECT 1'])).status).toBe(404);
});

test('clients keep syncing across a server restart', async () => {
    const dbPath = join(tempDir(), 'main.db');
    let server = await startServer([dbPath]);

    const writer = await connect({ path: ':memory:', url: server.url });
    await writer.exec('CREATE TABLE t(x)');
    await writer.exec('INSERT INTO t VALUES (1)');
    await writer.push();
    const reader = await connect({ path: ':memory:', url: server.url });
    expect(await rows(reader, 'SELECT x FROM t')).toEqual([{ x: 1 }]);

    await stopServer(server);
    server = await startServer([dbPath], server.port);

    await writer.exec('INSERT INTO t VALUES (2)');
    await writer.push();
    await reader.pull();
    expect(await rows(reader, 'SELECT x FROM t ORDER BY x')).toEqual([{ x: 1 }, { x: 2 }]);

    const fresh = await connect({ path: ':memory:', url: server.url });
    expect(await rows(fresh, 'SELECT x FROM t ORDER BY x')).toEqual([{ x: 1 }, { x: 2 }]);
});

test('metrics are served on their own listener', async () => {
    const dir = tempDir();
    const dbPath = join(dir, 'main.db');
    const metricsPort = await freePort();
    const { url } = await startServer([
        dbPath, '--auth-token', 'secret', '--backup-url', join(dir, 'backup'),
        '--metrics-listen', `127.0.0.1:${metricsPort}`,
    ]);
    expect((await pipeline(url, ['CREATE TABLE t(x)', 'INSERT INTO t VALUES (1)'], 'secret')).status).toBe(200);
    const onApiPort = await fetch(`${url}/metrics`, { headers: { authorization: 'Bearer secret' } });
    expect(onApiPort.status).toBe(404);

    const text = await (await fetch(`http://127.0.0.1:${metricsPort}/metrics`)).text();
    const value = (name: string) => Number(text.split('\n').find(line => line.startsWith(`${name}{`))?.split(' ').at(-1));
    expect(text).toContain('turso_sync_http_requests_total{endpoint="pipeline",status="200"} 1');
    expect(value('turso_sync_wal_frames')).toBeGreaterThan(0);
    expect(value('turso_sync_generation')).toBe(0);
    expect(value('turso_sync_backup_snapshot_pending')).toBeGreaterThanOrEqual(0);
});
