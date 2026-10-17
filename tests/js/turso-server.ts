import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import { request } from 'node:http';
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { afterAll } from 'vitest';

const ADMIN_URL = 'http://localhost:8081';

const fileDbDirs: string[] = [];
const fileDbServers: ChildProcess[] = [];
afterAll(async () => {
    if (process.env.SYNC_SERVER_BACKUP) {
        // Servers upload the rest of their WAL while shutting down.
        await Promise.all(fileDbServers.splice(0).map(c => c.exitCode === null ? once(c, 'exit') : null));
        for (const dir of fileDbDirs) verifyBackup(dir);
    }
    for (const dir of fileDbDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

/** A restore from the backup must reproduce the database file byte for byte. */
function verifyBackup(dir: string) {
    const database = join(dir, 'main.db');
    if (!existsSync(database)) return;
    const restored = join(dir, 'restored.db');
    execFileSync(process.env.LOCAL_SYNC_SERVER!, ['restore', '--backup-url', join(dir, 'backup'), restored]);
    // Reading through SQLite and closing folds the WAL into the database file.
    const db = new DatabaseSync(database);
    db.prepare('SELECT count(*) FROM sqlite_schema').get();
    db.close();
    if (!readFileSync(database).equals(readFileSync(restored))) {
        throw new Error(`restoring ${database} from its backup gives a different file`);
    }
}
const USER_URL = 'http://localhost:8080';

function randomStr(): string {
    return Math.random().toString(36).slice(2, 10);
}

function getFreePort(): Promise<number> {
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

interface HttpResponse {
    status: number;
    body: Buffer;
}

function httpPost(target: string, host: string | undefined, contentType: string, body: string): Promise<HttpResponse> {
    const url = new URL(target);
    return new Promise((resolve, reject) => {
        const req = request({
            hostname: url.hostname,
            port: url.port || (url.protocol === 'https:' ? 443 : 80),
            method: 'POST',
            path: `${url.pathname}${url.search}`,
            headers: {
                'Content-Type': contentType,
                'Content-Length': Buffer.byteLength(body).toString(),
                ...(host ? { 'Host': host } : {}),
            },
        }, res => {
            const chunks: Buffer[] = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => resolve({ status: res.statusCode ?? 0, body: Buffer.concat(chunks) }));
            res.on('error', reject);
        });
        req.on('error', reject);
        req.write(body);
        req.end();
    });
}

function httpGet(target: string): Promise<HttpResponse> {
    const url = new URL(target);
    return new Promise((resolve, reject) => {
        const req = request({
            hostname: url.hostname,
            port: url.port || (url.protocol === 'https:' ? 443 : 80),
            method: 'GET',
            path: `${url.pathname}${url.search}`,
        }, res => {
            const chunks: Buffer[] = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => resolve({ status: res.statusCode ?? 0, body: Buffer.concat(chunks) }));
            res.on('error', reject);
        });
        req.on('error', reject);
        req.end();
    });
}

async function ensureOk(resp: HttpResponse) {
    if (resp.status === 200) return;
    const text = resp.body.toString('utf-8');
    if (resp.status === 400 && text.includes('already exists')) return;
    throw new Error(`http failed: ${resp.status} ${text}`);
}

export class TursoServer {
    private constructor(
        private readonly _userUrl: string,
        private readonly _dbUrl: string,
        private readonly _host: string,
        private readonly _server: ChildProcess | null,
    ) { }

    static async create(opts: { args?: string[] } = {}): Promise<TursoServer> {
        const localSyncServer = process.env.LOCAL_SYNC_SERVER;
        if (localSyncServer) {
            const maxAttempts = 5;
            let lastErr: unknown = null;
            for (let attempt = 0; attempt < maxAttempts; attempt++) {
                const port = await getFreePort();
                let database = ':memory:';
                const backupArgs: string[] = [];
                if (process.env.SYNC_SERVER_FILE_DB) {
                    const dir = mkdtempSync(join(tmpdir(), 'turso-sync-suite-'));
                    fileDbDirs.push(dir);
                    database = join(dir, 'main.db');
                    if (process.env.SYNC_SERVER_BACKUP) backupArgs.push('--backup-url', join(dir, 'backup'));
                }
                const child = spawn(localSyncServer, [
                    database,
                    '--sync-server', `0.0.0.0:${port}`,
                    // enable experimental features on the server so synced clients
                    // can push schemas that use them (e.g. generated columns)
                    '--experimental-generated-columns',
                    ...backupArgs,
                    ...(opts.args ?? []),
                    ...(process.env.SYNC_SERVER_ARGS?.split(' ').filter(Boolean) ?? []),
                ], {
                    stdio: process.env.SYNC_SERVER_LOGS ? 'inherit' : 'ignore',
                });
                if (process.env.SYNC_SERVER_FILE_DB) fileDbServers.push(child);
                const userUrl = `http://localhost:${port}`;
                const deadline = Date.now() + 30000;
                let ready = false;
                while (Date.now() < deadline) {
                    if (child.exitCode !== null) break;
                    try {
                        await httpGet(userUrl);
                        ready = true;
                        break;
                    } catch {
                        await new Promise(r => setTimeout(r, 100));
                    }
                }
                if (ready) {
                    return new TursoServer(userUrl, userUrl, '', child);
                }
                child.kill();
                lastErr = new Error(`sync server did not become available within 30s (port ${port})`);
            }
            throw lastErr ?? new Error('failed to start local sync server');
        }
        const name = randomStr();
        const tokens = USER_URL.split('://');
        await ensureOk(await httpPost(`${ADMIN_URL}/v1/tenants/${name}`, undefined, 'application/json', ''));
        await ensureOk(await httpPost(`${ADMIN_URL}/v1/tenants/${name}/groups/${name}`, undefined, 'application/json', ''));
        await ensureOk(await httpPost(`${ADMIN_URL}/v1/tenants/${name}/groups/${name}/databases/${name}`, undefined, 'application/json', ''));
        return new TursoServer(
            USER_URL,
            `${tokens[0]}://${name}--${name}--${name}.${tokens[1]}`,
            `${name}--${name}--${name}.localhost`,
            null,
        );
    }

    dbUrl(): string {
        return this._dbUrl;
    }

    async dbSql(sql: string): Promise<unknown[][]> {
        const resp = await httpPost(
            `${this._userUrl}/v2/pipeline`,
            this._host || undefined,
            'application/json',
            JSON.stringify({ requests: [{ type: 'execute', stmt: { sql } }] }),
        );
        if (resp.status !== 200) {
            throw new Error(`http failed: ${resp.status} ${resp.body.toString('utf-8')}`);
        }
        const result = JSON.parse(resp.body.toString('utf-8'));
        if (result.results[0].type !== 'ok') {
            throw new Error(`remote sql execution failed: ${JSON.stringify(result)}`);
        }
        return result.results[0].response.result.rows.map((row: { value: unknown }[]) => row.map(cell => cell.value));
    }

    close(): void {
        if (this._server) this._server.kill();
    }
}
