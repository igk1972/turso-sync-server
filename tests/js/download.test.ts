import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { request, type IncomingMessage } from 'node:http';
import { join } from 'node:path';
import { expect, test } from 'vitest';
import { connect } from '@tursodatabase/sync';
import { NO_TIMER, checkpoint, rows, sql, startServer, tempDir } from './helpers.js';

const ROW_BYTES = 4000;

/** Rows of about a page each, inserted in parts to stay within short request timeouts. */
async function seed(url: string, count: number) {
    await sql(url, 'CREATE TABLE big(x INTEGER PRIMARY KEY, y BLOB)');
    for (let start = 1; start <= count; start += 2000) {
        const end = Math.min(count, start + 1999);
        await sql(url, `INSERT INTO big SELECT value, randomblob(${ROW_BYTES}) FROM generate_series(${start}, ${end})`);
    }
}

async function pageCount(url: string): Promise<number> {
    return Number((await sql(url, 'PRAGMA page_count'))[0][0].value);
}

/** A full bootstrap request whose response nobody reads until `readAll`. */
function startDownload(url: string): Promise<IncomingMessage> {
    return new Promise((resolve, reject) => {
        const req = request(`${url}/pull-updates`, {
            method: 'POST',
            headers: { 'content-type': 'application/protobuf', 'content-length': 0 },
        }, resolve);
        req.on('error', reject);
        req.end();
    });
}

function readAll(res: IncomingMessage): Promise<{ outcome: 'complete' | 'aborted', body: Buffer }> {
    return new Promise(resolve => {
        const chunks: Buffer[] = [];
        const done = (outcome: 'complete' | 'aborted') => resolve({ outcome, body: Buffer.concat(chunks) });
        res.on('data', chunk => chunks.push(chunk));
        res.on('end', () => done('complete'));
        res.on('error', () => done('aborted'));
        res.on('aborted', () => done('aborted'));
    });
}

/** Length-delimited protobuf messages in the body: the header, then one per page. */
function countMessages(body: Buffer): number {
    let offset = 0;
    let count = 0;
    while (offset < body.length) {
        let length = 0;
        let shift = 0;
        let byte: number;
        do {
            byte = body[offset++];
            length += (byte & 0x7f) * 2 ** shift;
            shift += 7;
        } while (byte & 0x80);
        offset += length;
        count++;
    }
    expect(offset).toBe(body.length);
    return count;
}

function rssMb(pid: number): number {
    const status = `/proc/${pid}/status`;
    const kb = existsSync(status)
        ? Number(/VmRSS:\s+(\d+)/.exec(readFileSync(status, 'utf8'))![1])
        : Number(execFileSync('ps', ['-o', 'rss=', '-p', String(pid)], { encoding: 'utf8' }).trim());
    return kb / 1024;
}

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

test('a large database downloads in batches while writes go on', async () => {
    const dbPath = join(tempDir(), 'main.db');
    const server = await startServer([dbPath, ...NO_TIMER]);
    const count = 16000;
    await seed(server.url, count);
    const pages = await pageCount(server.url);
    const before = rssMb(server.child.pid!);

    const download = await startDownload(server.url);
    await sleep(300);
    const started = Date.now();
    await sql(server.url, `INSERT INTO big VALUES (${count + 1}, randomblob(${ROW_BYTES}))`);
    expect(Date.now() - started).toBeLessThan(1000);
    expect(await checkpoint(server.url)).toEqual({ status: 'deferred' });
    // Buffering the whole response would take the size of the database, 62 MB.
    expect(rssMb(server.child.pid!) - before).toBeLessThan(24);

    const { outcome, body } = await readAll(download);
    expect(outcome).toBe('complete');
    expect(countMessages(body) - 1).toBe(pages);
    expect(await checkpoint(server.url)).toMatchObject({ status: 'done' });

    const client = await connect({ path: ':memory:', url: server.url });
    expect(await rows(client, 'SELECT count(*) AS c, sum(length(y)) AS bytes FROM big'))
        .toEqual([{ c: count + 1, bytes: (count + 1) * ROW_BYTES }]);
});

test('a download cut short by a WAL reset fails instead of ending early', async () => {
    const dbPath = join(tempDir(), 'main.db');
    const server = await startServer([dbPath, ...NO_TIMER]);
    await seed(server.url, 8000);

    const download = await startDownload(server.url);
    await sleep(300);
    await sql(server.url, 'PRAGMA wal_checkpoint(TRUNCATE)');
    expect((await readAll(download)).outcome).toBe('aborted');
});

test('a client that stops reading is dropped after the request timeout', async () => {
    const dbPath = join(tempDir(), 'main.db');
    const server = await startServer([dbPath, ...NO_TIMER, '--request-timeout', '1']);
    await seed(server.url, 8000);

    const download = await startDownload(server.url);
    await sleep(300);
    expect(await checkpoint(server.url)).toEqual({ status: 'deferred' });
    let status = 'deferred';
    for (let i = 0; i < 25 && status !== 'done'; i++) {
        await sleep(200);
        status = (await checkpoint(server.url)).status;
    }
    expect(status).toBe('done');
    expect((await readAll(download)).outcome).toBe('aborted');
});
