// Soak test: steady load on a running server for days, checking that clients
// and the backup stay consistent. Tunables are SOAK_* environment variables.
import { execFile } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { promisify } from 'node:util';
import { connect } from '@tursodatabase/sync';

const setting = (name: string, fallback: number) => Number(process.env[`SOAK_${name}`] ?? fallback);
const SYNC_URL = process.env.SOAK_URL ?? 'http://127.0.0.1:8000';
const TOKEN = process.env.TURSO_SYNC_AUTH_TOKEN;
const BACKUP_URL = process.env.SOAK_BACKUP_URL;
const SERVER_METRICS = process.env.SOAK_SERVER_METRICS ?? 'http://127.0.0.1:9464/metrics';
const METRICS_PORT = setting('METRICS_PORT', 9465);
const WRITERS = setting('WRITERS', 3);
const READERS = setting('READERS', 3);
const WRITE_INTERVAL_MS = setting('WRITE_INTERVAL_MS', 2000);
const PULL_INTERVAL_MS = setting('PULL_INTERVAL_MS', 1000);
const ROW_BYTES = setting('ROW_BYTES', 1024);
/** Rows each writer keeps: older ones are deleted in the same transaction. */
const WINDOW = setting('WINDOW', 1000);
const BURST_ROWS = setting('BURST_ROWS', 200);
const BURST_INTERVAL_S = setting('BURST_INTERVAL_S', 600);
const BOOTSTRAP_INTERVAL_S = setting('BOOTSTRAP_INTERVAL_S', 60);
const CHUNKED_EVERY = setting('CHUNKED_EVERY', 10);
const COMPARE_INTERVAL_S = setting('COMPARE_INTERVAL_S', 1800);
const RESTORE_INTERVAL_S = setting('RESTORE_INTERVAL_S', 10800);
/** A write acknowledged this long before a restore has to be in the backup. */
const BACKUP_LAG_S = setting('BACKUP_LAG_S', 30);
const STATUS_INTERVAL_S = setting('STATUS_INTERVAL_S', 300);
/** Clients checkpoint their local WAL past this size, as applications should. */
const CLIENT_WAL_BYTES = setting('CLIENT_WAL_MB', 4) * 2 ** 20;

/** Writer `w` owns rows `w * ID_SPAN + 1`, `+ 2`, … */
const ID_SPAN = 1e12;

type Db = Awaited<ReturnType<typeof connect>>;

interface Writer { w: number, db: Db, attempted: number, acked: number }
interface Reader { name: string, db: Db, seen: Map<number, number> }

const writers: Writer[] = [];
const readers: Reader[] = [];
/** Set while a comparison needs every client idle. */
let gate: Promise<void> | null = null;
let busy = 0;

// Metrics in the Prometheus text format.

const BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 1800];
const families = new Map<string, { type: string, help: string, series: Map<string, number> }>();

function family(name: string, type: string, help: string) {
    families.set(name, { type, help, series: new Map() });
}
family('soak_operations_total', 'counter', 'Client operations and checks by result');
family('soak_operation_seconds', 'histogram', 'Time taken by client operations and checks');
family('soak_violations_total', 'counter', 'Consistency violations by check');
family('soak_rows_acked', 'gauge', 'Last row of each writer acknowledged by the server');
family('soak_last_ok_timestamp_seconds', 'gauge', 'When a comparison or a restore check last passed');
family('process_resident_memory_bytes', 'gauge', 'Resident memory size');
family('process_cpu_seconds_total', 'counter', 'User and system CPU time spent');
family('nodejs_heap_size_used_bytes', 'gauge', 'JavaScript heap in use; the rest of the resident memory is mostly native');

function series(name: string, labels: Record<string, string | number>, suffix = '') {
    const text = Object.entries(labels).map(([k, v]) => `${k}="${v}"`).join(',');
    return `${name}${suffix}{${text}}`;
}

function add(name: string, labels: Record<string, string | number>, by = 1, suffix = '') {
    const all = families.get(name)!.series;
    const key = series(name, labels, suffix);
    all.set(key, (all.get(key) ?? 0) + by);
}

function set(name: string, labels: Record<string, string | number>, value: number) {
    families.get(name)!.series.set(series(name, labels), value);
}

function observe(op: string, seconds: number) {
    for (const le of [...BUCKETS, '+Inf']) {
        add('soak_operation_seconds', { op, le }, typeof le === 'number' && seconds > le ? 0 : 1, '_bucket');
    }
    add('soak_operation_seconds', { op }, seconds, '_sum');
    add('soak_operation_seconds', { op }, 1, '_count');
}

function exposition(): string {
    const memory = process.memoryUsage();
    set('process_resident_memory_bytes', {}, memory.rss);
    set('nodejs_heap_size_used_bytes', {}, memory.heapUsed);
    const cpu = process.cpuUsage();
    set('process_cpu_seconds_total', {}, (cpu.user + cpu.system) / 1e6);
    let text = '';
    for (const [name, { type, help, series }] of families) {
        text += `# HELP ${name} ${help}\n# TYPE ${name} ${type}\n`;
        for (const [key, value] of series) text += `${key} ${value}\n`;
    }
    return text;
}

function total(name: string, match: string): number {
    let sum = 0;
    for (const [key, value] of families.get(name)!.series) if (key.includes(match)) sum += value;
    return sum;
}

// Logging, with repeated errors of one kind folded into one line a minute.

function log(message: string) {
    console.log(`${new Date().toISOString()} ${message}`);
}

const errorLog = new Map<string, { at: number, suppressed: number }>();

function logError(op: string, err: unknown) {
    const entry = errorLog.get(op);
    const now = Date.now();
    if (entry && now - entry.at < 60_000) {
        entry.suppressed++;
        return;
    }
    const more = entry?.suppressed ? ` (${entry.suppressed} more since the last report)` : '';
    log(`error in ${op}: ${err instanceof Error ? err.message : err}${more}`);
    errorLog.set(op, { at: now, suppressed: 0 });
}

let violationLines = 0;

function violation(check: string, message: string) {
    add('soak_violations_total', { check });
    if (violationLines++ < 1000) log(`VIOLATION ${check}: ${message}`);
}

const reported = new WeakSet<object>();

async function timed<T>(op: string, action: () => Promise<T>): Promise<T> {
    const start = performance.now();
    try {
        const result = await action();
        add('soak_operations_total', { op, result: 'ok' });
        return result;
    } catch (err) {
        add('soak_operations_total', { op, result: 'error' });
        logError(op, err);
        if (err instanceof Object) reported.add(err);
        throw err;
    } finally {
        observe(op, (performance.now() - start) / 1000);
    }
}

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const jitter = (ms: number) => ms * (0.5 + Math.random());

async function retry<T>(op: string, action: () => Promise<T>): Promise<T> {
    for (;;) {
        try {
            return await action();
        } catch (err) {
            logError(op, err);
            await sleep(2000);
        }
    }
}

/** Runs `step` forever, the first time after a shorter pause to catch a broken setup early. */
async function forever(op: string, pauseMs: () => number, step: () => Promise<void>, firstMs = pauseMs()) {
    for (let pause = firstMs; ; pause = pauseMs()) {
        await sleep(pause);
        try {
            await step();
        } catch (err) {
            if (!(err instanceof Object && reported.has(err))) logError(op, err);
        }
    }
}

// Database helpers.

function client(name: string, extra: Record<string, unknown> = {}): Promise<Db> {
    return connect({ path: ':memory:', url: SYNC_URL, authToken: TOKEN, clientName: name, ...extra });
}

async function values(db: Db): Promise<number[]> {
    return (await (await db.prepare('SELECT x FROM t ORDER BY x')).all()).map((row: any) => row.x);
}

async function digest(db: Db): Promise<string> {
    const hash = createHash('sha256');
    for (const row of await (await db.prepare('SELECT x, y FROM t ORDER BY x')).all()) {
        hash.update(`${row.x}:${row.y};`);
    }
    return hash.digest('hex');
}

/**
 * The last row of each writer. Writes are applied in order, so any consistent
 * state holds, per writer, an unbroken run of its last min(top, WINDOW) rows.
 */
function tops(check: string, xs: number[]): Map<number, number> {
    const runs = new Map<number, { first: number, last: number, count: number }>();
    for (const x of xs) {
        const w = Math.floor(x / ID_SPAN);
        const seq = x - w * ID_SPAN;
        const run = runs.get(w);
        if (run) {
            run.last = seq;
            run.count++;
        } else {
            runs.set(w, { first: seq, last: seq, count: 1 });
        }
    }
    const result = new Map<number, number>();
    for (const [w, { first, last, count }] of runs) {
        if (last - first + 1 !== count || count !== Math.min(last, WINDOW)) {
            violation(check, `writer ${w} has ${count} rows from ${first} to ${last}`);
        }
        result.set(w, last);
    }
    return result;
}

/** Without it the local WAL, kept in memory here, grows for good. */
async function keepWalSmall(db: Db) {
    if ((await db.stats()).mainWalSize > CLIENT_WAL_BYTES) await timed('client_checkpoint', () => db.checkpoint());
}

function acked(): Map<number, number> {
    return new Map(writers.map(writer => [writer.w, writer.acked]));
}

/** Rows the server acknowledged before `floor` was taken are there; rows never written are not. */
function checkBounds(check: string, held: Map<number, number>, floor: Map<number, number>) {
    for (const writer of writers) {
        const top = held.get(writer.w) ?? 0;
        if (top < floor.get(writer.w)!) {
            violation(check, `writer ${writer.w} ends at ${top}, the server acknowledged ${floor.get(writer.w)}`);
        }
        if (top > writer.attempted) {
            violation(check, `writer ${writer.w} ends at ${top}, only ${writer.attempted} were written`);
        }
    }
}

// Workers.

async function idle<T>(action: () => Promise<T>): Promise<T> {
    while (gate) await gate;
    busy++;
    try {
        return await action();
    } finally {
        busy--;
    }
}

async function startWriter(w: number): Promise<Writer> {
    const db = await client(`writer-${w}`);
    const base = w * ID_SPAN;
    const [row] = await (await db.prepare(`SELECT max(x) AS top FROM t WHERE x > ${base} AND x < ${base + ID_SPAN}`)).all();
    const top = row.top === null ? 0 : row.top - base;
    set('soak_rows_acked', { writer: w }, top);
    return { w, db, attempted: top, acked: top };
}

async function push(writer: Writer, op = 'push') {
    const target = writer.attempted;
    await timed(op, () => writer.db.push());
    writer.acked = target;
    set('soak_rows_acked', { writer: writer.w }, target);
}

async function runWriter(writer: Writer) {
    let nextBurst = Date.now() + jitter(BURST_INTERVAL_S * 1000);
    await forever(`writer-${writer.w}`, () => jitter(WRITE_INTERVAL_MS), () => idle(async () => {
        const burst = Date.now() >= nextBurst;
        if (burst) nextBurst = Date.now() + jitter(BURST_INTERVAL_S * 1000);
        const base = writer.w * ID_SPAN;
        const first = writer.attempted + 1;
        const last = writer.attempted + (burst ? BURST_ROWS : 1);
        const rows = Array.from({ length: last - first + 1 }, (_, i) => `(${base + first + i}, hex(randomblob(${ROW_BYTES / 2})))`);
        try {
            await writer.db.exec(`BEGIN; INSERT INTO t VALUES ${rows.join(', ')}; `
                + `DELETE FROM t WHERE x > ${base} AND x <= ${base + last - WINDOW}; COMMIT;`);
        } catch (err) {
            await writer.db.exec('ROLLBACK').catch(() => {});
            throw err;
        }
        writer.attempted = last;
        // A failed push leaves the rows pending; the next one sends them.
        await push(writer, burst ? 'burst' : 'push');
        await keepWalSmall(writer.db);
    }));
}

async function runReader(reader: Reader) {
    let pulls = 0;
    let failures = 0;
    await forever(reader.name, () => jitter(PULL_INTERVAL_MS), () => idle(async () => {
        try {
            await timed('pull', () => reader.db.pull());
            failures = 0;
        } catch (err) {
            if (++failures < 10) return;
            failures = 0;
            await reader.db.close().catch(() => {});
            reader.db = await timed('reconnect', () => client(reader.name));
        }
        await keepWalSmall(reader.db);
        if (++pulls % 10 !== 0) return;
        const held = tops('reader', await values(reader.db));
        for (const [w, top] of held) {
            const seen = reader.seen.get(w) ?? 0;
            if (top < seen) violation('reader', `${reader.name}: writer ${w} went back from ${seen} to ${top}`);
            reader.seen.set(w, Math.max(top, seen));
        }
        checkBounds('reader', held, new Map(writers.map(writer => [writer.w, 0])));
    }));
}

async function runBootstraps() {
    let count = 0;
    await forever('bootstrap', () => jitter(BOOTSTRAP_INTERVAL_S * 1000), async () => {
        const floor = acked();
        // A chunked download pins a revision, holding off checkpoints for a while.
        const chunked = CHUNKED_EVERY > 0 && ++count % CHUNKED_EVERY === 0;
        const db = await timed(chunked ? 'bootstrap_chunked' : 'bootstrap',
            () => client('bootstrap', chunked ? { pullBytesThreshold: 256 * 1024 } : {}));
        try {
            checkBounds('bootstrap', tops('bootstrap', await values(db)), floor);
        } finally {
            await db.close();
        }
    });
}

/** With writes paused and pushed, every client pulls to the same state as a fresh one. */
async function compare() {
    let release = () => {};
    gate = new Promise(resolve => release = resolve);
    try {
        while (busy > 0) await sleep(50);
        for (const writer of writers) if (writer.acked < writer.attempted) await push(writer);
        const fresh = await client('compare');
        try {
            const held = tops('compare', await values(fresh));
            for (const writer of writers) {
                const top = held.get(writer.w) ?? 0;
                if (top !== writer.attempted) violation('compare', `writer ${writer.w} ends at ${top}, wrote ${writer.attempted}`);
            }
            const expected = await digest(fresh);
            const clients = [
                ...writers.map(writer => ({ name: `writer-${writer.w}`, db: writer.db })),
                ...readers,
            ];
            for (const { name, db } of clients) {
                await db.pull();
                if (await digest(db) !== expected) violation('compare', `${name} differs from a fresh client`);
            }
        } finally {
            await fresh.close();
        }
        set('soak_last_ok_timestamp_seconds', { check: 'compare' }, Date.now() / 1000);
    } finally {
        gate = null;
        release();
    }
}

/** Restores the backup next to the running server and checks it holds the acknowledged writes. */
async function restore() {
    const floor = acked();
    await sleep(BACKUP_LAG_S * 1000);
    const dir = mkdtempSync(join(tmpdir(), 'soak-restore-'));
    try {
        const path = join(dir, 'restored.db');
        await promisify(execFile)('turso-sync-server', ['restore', '--backup-url', BACKUP_URL!, path]);
        const db = new DatabaseSync(path);
        try {
            const { integrity_check } = db.prepare('PRAGMA integrity_check').get() as { integrity_check: string };
            if (integrity_check !== 'ok') violation('restore', `integrity check: ${integrity_check}`);
            const xs = db.prepare('SELECT x FROM t ORDER BY x').all().map((row: any) => row.x);
            checkBounds('restore', tops('restore', xs), floor);
        } finally {
            db.close();
        }
        set('soak_last_ok_timestamp_seconds', { check: 'restore' }, Date.now() / 1000);
    } finally {
        rmSync(dir, { recursive: true, force: true });
    }
}

async function serverStatus(): Promise<string> {
    const text = await (await fetch(SERVER_METRICS)).text();
    const sum = (name: string, match = '') => text.split('\n')
        .filter(line => line.startsWith(`${name}{`) || line.startsWith(`${name} `))
        .filter(line => line.includes(match))
        .reduce((acc, line) => acc + Number(line.split(' ').at(-1)), 0);
    const lag = Date.now() / 1000 - sum('turso_sync_backup_synced_timestamp_seconds');
    return `server rss ${(sum('process_resident_memory_bytes') / 2 ** 20).toFixed(0)} MiB, `
        + `fds ${sum('process_open_fds')}, wal ${sum('turso_sync_wal_frames')} frames, `
        + `generation ${sum('turso_sync_generation')}, backup lag ${lag.toFixed(1)} s, `
        + `uploads ${sum('turso_sync_backup_uploads_total', 'result="ok"')} ok `
        + `${sum('turso_sync_backup_uploads_total', 'result="error"')} failed`;
}

async function status() {
    const ops = ['push', 'burst', 'pull', 'client_checkpoint', 'bootstrap', 'bootstrap_chunked', 'compare', 'restore']
        .map(op => `${op} ${total('soak_operations_total', `op="${op}",result="ok"`)}`
            + `/${total('soak_operations_total', `op="${op}",result="error"`)}`)
        .join(', ');
    const rows = writers.reduce((sum, writer) => sum + writer.acked, 0);
    const server = await serverStatus().catch(err => `server metrics unavailable: ${err.message}`);
    log(`rows ${rows}, ok/failed: ${ops}, violations ${total('soak_violations_total', '')}; ${server}`);
}

for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => process.exit(0));

createServer((_, res) => {
    res.writeHead(200, { 'content-type': 'text/plain; version=0.0.4' });
    res.end(exposition());
}).listen(METRICS_PORT, '127.0.0.1');

log(`soak on ${SYNC_URL}: ${WRITERS} writers, ${READERS} readers, backup ${BACKUP_URL ?? 'not checked'}`);
await retry('init', async () => {
    const db = await client('init');
    await db.exec('CREATE TABLE IF NOT EXISTS t(x INTEGER PRIMARY KEY, y TEXT)');
    await db.push();
    await db.close();
});
for (let w = 1; w <= WRITERS; w++) writers.push(await retry('init', () => startWriter(w)));
for (let i = 1; i <= READERS; i++) {
    const name = `reader-${i}`;
    readers.push({ name, db: await retry('init', () => client(name)), seen: new Map() });
}
log(`started from ${writers.map(writer => writer.acked).join(', ')} rows per writer`);

await Promise.all([
    ...writers.map(runWriter),
    ...readers.map(runReader),
    runBootstraps(),
    forever('compare', () => COMPARE_INTERVAL_S * 1000, () => timed('compare', compare),
        Math.min(COMPARE_INTERVAL_S, 300) * 1000),
    BACKUP_URL && forever('restore', () => RESTORE_INTERVAL_S * 1000, () => timed('restore', restore),
        Math.min(RESTORE_INTERVAL_S, 600) * 1000),
    forever('status', () => STATUS_INTERVAL_S * 1000, status),
]);
