// Load test: one server with --sync-dir, many databases, writers and readers
// spread over worker processes. Prints throughput, latencies, the server's
// memory and CPU, and checks that every acknowledged write is on the server.
// Tunables are LOAD_* environment variables.
import { execFileSync, spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { connect } from '@tursodatabase/sync';

const setting = (name: string, fallback: number) => Number(process.env[`LOAD_${name}`] ?? fallback);
const DBS = setting('DBS', 20);
const WRITERS = setting('WRITERS_PER_DB', 2);
const READERS = setting('READERS_PER_DB', 8);
const WRITE_INTERVAL_MS = setting('WRITE_INTERVAL_MS', 1000);
const PULL_INTERVAL_MS = setting('PULL_INTERVAL_MS', 1000);
const ROW_BYTES = setting('ROW_BYTES', 1024);
const WINDOW = setting('WINDOW', 1000);
const DURATION_S = setting('DURATION_S', 120);
const PROCESSES = setting('PROCESSES', 4);
const SERVER = process.env.LOAD_SERVER ?? join(import.meta.dirname, '../../target/release/turso-sync-server');
const SERVER_ARGS = (process.env.LOAD_SERVER_ARGS ?? '--checkpoint-interval 30').split(' ').filter(Boolean);
const TOKEN = 'load';
const ID_SPAN = 1e9;

interface Report {
    latencies: Record<string, number[]>;
    errors: Record<string, number>;
    acked: [db: number, writer: number, last: number][];
}

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
const jitter = (ms: number) => ms * (0.5 + Math.random());

if (process.env.LOAD_WORKER) await worker(Number(process.env.LOAD_WORKER) - 1, process.env.LOAD_URL!);
else await driver();

async function worker(index: number, url: string) {
    const report: Report = { latencies: {}, errors: {}, acked: [] };
    const record = (op: string, started: number) => (report.latencies[op] ??= []).push(performance.now() - started);
    const fail = (op: string, err: unknown) => {
        report.errors[op] = (report.errors[op] ?? 0) + 1;
        if (report.errors[op] <= 3) console.error(`worker ${index + 1}: ${op} failed: ${err}`);
    };
    const deadline = Date.now() + DURATION_S * 1000;
    const clients: Promise<void>[] = [];
    const perDb = WRITERS + READERS;
    for (let i = index; i < DBS * perDb; i += PROCESSES) {
        const db = Math.floor(i / perDb);
        const slot = i % perDb;
        clients.push(slot < WRITERS ? writer(db, slot + 1) : reader(db));
    }
    await Promise.all(clients);
    process.stdout.write(JSON.stringify(report));

    async function open(db: number) {
        const started = performance.now();
        for (;;) {
            try {
                const client = await connect({ path: ':memory:', url: `${url}/db/d${db}`, authToken: TOKEN });
                record('bootstrap', started);
                return client;
            } catch (err) {
                fail('bootstrap', err);
                await sleep(1000);
            }
        }
    }

    async function keepWalSmall(client: Awaited<ReturnType<typeof connect>>) {
        if ((await client.stats()).mainWalSize > 4 * 2 ** 20) await client.checkpoint();
    }

    async function writer(db: number, w: number) {
        await sleep(Math.random() * WRITE_INTERVAL_MS);
        const client = await open(db);
        await client.exec('CREATE TABLE IF NOT EXISTS t(x INTEGER PRIMARY KEY, y TEXT)');
        const base = w * ID_SPAN;
        let seq = 0;
        let acked = 0;
        while (Date.now() < deadline) {
            seq++;
            await client.exec(`BEGIN; INSERT INTO t VALUES (${base + seq}, hex(randomblob(${ROW_BYTES / 2})));
                DELETE FROM t WHERE x > ${base} AND x <= ${base + seq - WINDOW}; COMMIT;`);
            const started = performance.now();
            try {
                await client.push();
                record('push', started);
                acked = seq;
            } catch (err) {
                fail('push', err);
            }
            await keepWalSmall(client);
            await sleep(jitter(WRITE_INTERVAL_MS));
        }
        for (let i = 0; i < 10 && acked < seq; i++) {
            try {
                await client.push();
                acked = seq;
            } catch {
                await sleep(500);
            }
        }
        report.acked.push([db, w, acked]);
        await client.close();
    }

    async function reader(db: number) {
        await sleep(Math.random() * PULL_INTERVAL_MS);
        const client = await open(db);
        while (Date.now() < deadline) {
            const started = performance.now();
            try {
                await client.pull();
                record('pull', started);
            } catch (err) {
                fail('pull', err);
            }
            await keepWalSmall(client);
            await sleep(jitter(PULL_INTERVAL_MS));
        }
        await client.close();
    }
}

async function driver() {
    const dir = mkdtempSync(join(tmpdir(), 'turso-load-'));
    const [port, metricsPort] = [await freePort(), await freePort()];
    const server = spawn(SERVER, [
        '--sync-dir', dir, '--listen', `127.0.0.1:${port}`, '--metrics-listen', `127.0.0.1:${metricsPort}`,
        ...SERVER_ARGS,
    ], { env: { ...process.env, TURSO_SYNC_AUTH_TOKEN: TOKEN, RUST_LOG: 'warn' }, stdio: ['ignore', 'inherit', 'inherit'] });
    const url = `http://127.0.0.1:${port}`;
    await waitForHealth(url);
    const idle = usage(server.pid!);

    const samples: { rss: number, cpu: number, at: number }[] = [];
    const sampler = setInterval(() => samples.push({ ...usage(server.pid!), at: performance.now() }), 5000);
    const started = performance.now();
    const reports = await Promise.all(Array.from({ length: PROCESSES }, (_, i) => runWorker(i, url)));
    const elapsed = (performance.now() - started) / 1000;
    clearInterval(sampler);
    const busy = usage(server.pid!);

    const violations = await verify(url, reports.flatMap(r => r.acked));
    const metrics = await (await fetch(`http://127.0.0.1:${metricsPort}/metrics`)).text();
    server.kill('SIGTERM');
    await new Promise(r => server.once('exit', r));
    const diskMb = Number(execFileSync('du', ['-sk', dir], { encoding: 'utf8' }).split('\t')[0]) / 1024;
    rmSync(dir, { recursive: true, force: true });

    const latencies: Record<string, number[]> = {};
    const errors: Record<string, number> = {};
    for (const report of reports) {
        for (const [op, values] of Object.entries(report.latencies)) (latencies[op] ??= []).push(...values);
        for (const [op, count] of Object.entries(report.errors)) errors[op] = (errors[op] ?? 0) + count;
    }
    const runtime = process.versions.bun ? `bun ${process.versions.bun}` : `node ${process.versions.node}`;
    console.log(`${DBS} databases × (${WRITERS} writers every ${WRITE_INTERVAL_MS} ms + ${READERS} readers every ${PULL_INTERVAL_MS} ms), `
        + `${ROW_BYTES} B rows, ${PROCESSES} ${runtime} processes, ${elapsed.toFixed(0)} s, server ${SERVER_ARGS.join(' ')}`);
    console.log('op         count    per s    p50 ms   p95 ms   p99 ms   max ms   errors');
    for (const [op, values] of Object.entries(latencies).sort()) {
        values.sort((a, b) => a - b);
        const at = (q: number) => values[Math.min(values.length - 1, Math.floor(q * values.length))].toFixed(0);
        console.log(`${op.padEnd(10)} ${String(values.length).padStart(6)} ${(values.length / elapsed).toFixed(1).padStart(8)} `
            + `${at(0.5).padStart(9)} ${at(0.95).padStart(8)} ${at(0.99).padStart(8)} ${at(1).padStart(8)} ${String(errors[op] ?? 0).padStart(8)}`);
    }
    const peak = Math.max(idle.rss, ...samples.map(s => s.rss));
    const cpu = (busy.cpu - idle.cpu) / elapsed * 100;
    const checkpoints = [...metrics.matchAll(/^turso_sync_checkpoints_total\{.*outcome="(\w+)".*\} (\d+)/gm)]
        .reduce((sum: Record<string, number>, [, outcome, count]) => ({ ...sum, [outcome]: (sum[outcome] ?? 0) + Number(count) }), {});
    const walMax = Math.max(0, ...[...metrics.matchAll(/^turso_sync_wal_frames\{.*\} (\d+)/gm)].map(m => Number(m[1])));
    console.log(`server: rss idle ${idle.rss.toFixed(0)} MB, peak ${peak.toFixed(0)} MB (${((peak - idle.rss) / DBS).toFixed(1)} MB per database), `
        + `cpu ${cpu.toFixed(0)}% of a core, disk ${diskMb.toFixed(0)} MB, wal max ${walMax} frames, checkpoints ${JSON.stringify(checkpoints)}`);
    console.log(`lost acknowledged writes: ${violations}`);
    if (violations > 0) process.exitCode = 1;
}

function runWorker(index: number, url: string): Promise<Report> {
    return new Promise((resolve, reject) => {
        const child = spawn(process.execPath, [...process.execArgv, process.argv[1]], {
            env: { ...process.env, LOAD_WORKER: String(index + 1), LOAD_URL: url },
            stdio: ['ignore', 'pipe', 'inherit'],
        });
        let output = '';
        child.stdout.on('data', chunk => output += chunk);
        child.on('exit', code => code === 0 ? resolve(JSON.parse(output)) : reject(new Error(`worker ${index + 1} exited with ${code}`)));
    });
}

/** Every writer's rows on the server end at the last write it saw acknowledged. */
async function verify(url: string, acked: Report['acked']): Promise<number> {
    let violations = 0;
    for (let db = 0; db < DBS; db++) {
        const client = await connect({ path: ':memory:', url: `${url}/db/d${db}`, authToken: TOKEN });
        for (const [, w, last] of acked.filter(([d]) => d === db)) {
            const [row] = await (await client.prepare(
                `SELECT max(x) AS top, count(*) AS n FROM t WHERE x > ${w * ID_SPAN} AND x < ${(w + 1) * ID_SPAN}`)).all() as any[];
            const top = row.top == null ? 0 : row.top - w * ID_SPAN;
            if (top < last || row.n !== Math.min(top, WINDOW)) {
                console.log(`database d${db}, writer ${w}: acknowledged ${last}, server holds ${row.n} rows up to ${top}`);
                violations++;
            }
        }
        await client.close();
    }
    return violations;
}

/** Resident memory in MB and CPU time in seconds of a process. */
function usage(pid: number): { rss: number, cpu: number } {
    const [rss, time] = execFileSync('ps', ['-o', 'rss=,time=', '-p', String(pid)], { encoding: 'utf8' }).trim().split(/\s+/);
    const [days, clock] = time.includes('-') ? time.split('-') : ['0', time];
    const cpu = Number(days) * 86400 + clock.split(':').map(Number).reduce((total, part) => total * 60 + part, 0);
    return { rss: Number(rss) / 1024, cpu };
}

function freePort(): Promise<number> {
    return new Promise((resolve, reject) => {
        const probe = createServer().listen(0, '127.0.0.1', () => {
            const { port } = probe.address() as { port: number };
            probe.close(() => resolve(port));
        }).on('error', reject);
    });
}

async function waitForHealth(url: string) {
    for (let i = 0; i < 100; i++) {
        try {
            if ((await fetch(`${url}/health`)).ok) return;
        } catch {}
        await sleep(100);
    }
    throw new Error('the server did not start');
}
