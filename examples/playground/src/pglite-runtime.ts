import { drizzle } from "drizzle-orm/pglite";
import type { RedisLike, RedisPipelineLike } from "@tupl/provider-ioredis";
import type { QueryRow } from "@tupl/schema";
import { REDIS_INPUT_TABLE_NAME, type RedisInputRow } from "./redis-provider";
import type {
  DownstreamRows,
  ExecutedProviderOperation,
  ExecutedRedisLookupProviderOperation,
  ExecutedSqlProviderOperation,
} from "./types";
import {
  orderItemsTable,
  ordersTable,
  orgsTable,
  productsTable,
  userProductAccessTable,
  usersTable,
  vendorsTable,
} from "./downstream-model";

interface PlaygroundPgliteRuntime {
  client: PGliteClient;
  db: ReturnType<typeof drizzle>;
}

interface PlaygroundRedisRuntime {
  redis: PlaygroundRedisClient;
}

interface PlaygroundRedisClient extends RedisLike {
  flushall(): Promise<unknown>;
  hset(key: string, ...args: string[]): Promise<unknown>;
}

class InMemoryRedisPipeline implements RedisPipelineLike {
  private readonly keys: string[] = [];

  constructor(private readonly hashes: Map<string, Map<string, string>>) {}

  hgetall(key: string): InMemoryRedisPipeline {
    this.keys.push(key);
    return this;
  }

  async exec(): Promise<Array<[Error | null, Record<string, string>] | null>> {
    return this.keys.map((key) => {
      const hash = this.hashes.get(key);
      return [null, hash ? Object.fromEntries(hash.entries()) : {}];
    });
  }
}

class InMemoryRedisClient implements PlaygroundRedisClient {
  private readonly hashes = new Map<string, Map<string, string>>();

  pipeline(): InMemoryRedisPipeline {
    return new InMemoryRedisPipeline(this.hashes);
  }

  async flushall(): Promise<unknown> {
    this.hashes.clear();
    return "OK";
  }

  async hset(key: string, ...args: string[]): Promise<unknown> {
    if (args.length % 2 !== 0) {
      throw new Error("hset expects field/value pairs.");
    }

    const hash = this.hashes.get(key) ?? new Map<string, string>();
    let created = 0;
    for (let index = 0; index < args.length; index += 2) {
      const field = args[index];
      const value = args[index + 1];
      if (typeof field !== "string" || typeof value !== "string") {
        throw new Error("hset expects string field/value pairs.");
      }
      if (!hash.has(field)) {
        created += 1;
      }
      hash.set(field, value);
    }
    this.hashes.set(key, hash);
    return created;
  }
}

let pgliteRuntimePromise: Promise<PlaygroundPgliteRuntime> | null = null;
let redisRuntimePromise: Promise<PlaygroundRedisRuntime> | null = null;
let pgliteCtorPromise: Promise<PGliteConstructor> | null = null;
let redisCtorPromise: Promise<RedisConstructor> | null = null;
const executedProviderOperations: ExecutedProviderOperation[] = [];
let nextOperationId = 1;

const PGLITE_CDN_URL = "https://cdn.jsdelivr.net/npm/@electric-sql/pglite/dist/index.js";

interface PGliteClient {
  exec(statement: string): Promise<unknown>;
}

type PGliteConstructor = new () => PGliteClient;
type RedisConstructor = new () => PlaygroundRedisClient;

type NewExecutedProviderOperation =
  | Omit<ExecutedSqlProviderOperation, "id" | "timestamp">
  | Omit<ExecutedRedisLookupProviderOperation, "id" | "timestamp">;

function makeOperationId(): string {
  const id = `op_${nextOperationId}`;
  nextOperationId += 1;
  return id;
}

export function recordExecutedProviderOperation(
  operation: NewExecutedProviderOperation,
): ExecutedProviderOperation {
  const id = makeOperationId();
  const timestamp = Date.now();
  const entry =
    operation.kind === "sql_query"
      ? ({
          ...operation,
          id,
          timestamp,
        } satisfies ExecutedSqlProviderOperation)
      : ({
          ...operation,
          id,
          timestamp,
        } satisfies ExecutedRedisLookupProviderOperation);
  executedProviderOperations.push(entry);
  return entry;
}

const CREATE_SCHEMA_STATEMENTS = [
  `DO $$ BEGIN CREATE TYPE user_role AS ENUM ('buyer', 'manager'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;`,
  `DO $$ BEGIN CREATE TYPE vendor_tier AS ENUM ('standard', 'preferred'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;`,
  `DO $$ BEGIN CREATE TYPE product_category AS ENUM ('hardware', 'software', 'services'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;`,
  `DO $$ BEGIN CREATE TYPE order_status AS ENUM ('pending', 'paid', 'shipped'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;`,
  `CREATE TABLE IF NOT EXISTS orgs (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL
  );`,
  `CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY NOT NULL,
    org_id TEXT NOT NULL,
    email TEXT NOT NULL,
    display_name TEXT NOT NULL,
    role user_role NOT NULL,
    FOREIGN KEY (org_id) REFERENCES orgs(id)
  );`,
  `CREATE TABLE IF NOT EXISTS vendors (
    id TEXT PRIMARY KEY NOT NULL,
    org_id TEXT NOT NULL,
    name TEXT NOT NULL,
    tier vendor_tier NOT NULL,
    FOREIGN KEY (org_id) REFERENCES orgs(id)
  );`,
  `CREATE TABLE IF NOT EXISTS products (
    id TEXT PRIMARY KEY NOT NULL,
    org_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    name TEXT NOT NULL,
    category product_category NOT NULL,
    active BOOLEAN NOT NULL,
    FOREIGN KEY (org_id) REFERENCES orgs(id)
  );`,
  `CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY NOT NULL,
    org_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    vendor_id TEXT NOT NULL,
    status order_status NOT NULL,
    total_cents NUMERIC(12,0) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (org_id) REFERENCES orgs(id),
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (vendor_id) REFERENCES vendors(id)
  );`,
  `CREATE TABLE IF NOT EXISTS order_items (
    id TEXT PRIMARY KEY NOT NULL,
    org_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    line_total_cents INTEGER NOT NULL,
    FOREIGN KEY (org_id) REFERENCES orgs(id),
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (order_id) REFERENCES orders(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
  );`,
  `CREATE TABLE IF NOT EXISTS user_product_access (
    id TEXT PRIMARY KEY NOT NULL,
    user_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
  );`,
];

const DROP_SCHEMA_STATEMENTS = [
  "DROP TABLE IF EXISTS user_product_access;",
  "DROP TABLE IF EXISTS order_items;",
  "DROP TABLE IF EXISTS orders;",
  "DROP TABLE IF EXISTS products;",
  "DROP TABLE IF EXISTS vendors;",
  "DROP TABLE IF EXISTS users;",
  "DROP TABLE IF EXISTS orgs;",
  "DROP TYPE IF EXISTS order_status;",
  "DROP TYPE IF EXISTS product_category;",
  "DROP TYPE IF EXISTS vendor_tier;",
  "DROP TYPE IF EXISTS user_role;",
];

async function execStatements(client: PGliteClient, statements: readonly string[]): Promise<void> {
  for (const statement of statements) {
    await client.exec(statement);
  }
}

async function insertRowsSerial(
  db: PlaygroundPgliteRuntime["db"],
  table: unknown,
  rows: QueryRow[],
): Promise<void> {
  for (const row of rows) {
    await db
      .insert(table as never)
      .values(row as never)
      .execute();
  }
}

async function seedRedisHashes(redis: PlaygroundRedisClient, rows: RedisInputRow[]): Promise<void> {
  await redis.flushall();

  for (const row of rows) {
    await redis.hset(
      `product_view_counts:${row.user_id}:${row.product_id}`,
      "product_id",
      row.product_id,
      "view_count",
      String(row.view_count),
    );
  }
}

function readRedisInputRows(rows: DownstreamRows): RedisInputRow[] {
  return ((rows[REDIS_INPUT_TABLE_NAME] ?? []) as Array<Record<string, unknown>>).flatMap((row) => {
    const userId = row.user_id;
    const productId = row.product_id;
    const viewCount = row.view_count;
    if (
      typeof userId !== "string" ||
      userId.trim().length === 0 ||
      typeof productId !== "string" ||
      productId.trim().length === 0 ||
      typeof viewCount !== "number" ||
      !Number.isFinite(viewCount)
    ) {
      return [];
    }

    return [
      {
        user_id: userId,
        product_id: productId,
        view_count: Math.trunc(viewCount),
      },
    ];
  });
}

async function createPgliteRuntime(): Promise<PlaygroundPgliteRuntime> {
  const PGlite = await loadPGliteConstructor();
  const client = new PGlite();

  const logger = {
    logQuery(query: string, params: unknown[]): void {
      recordExecutedProviderOperation({
        kind: "sql_query",
        provider: "dbProvider",
        sql: query,
        variables: params,
      });
    },
  };

  return {
    client,
    db: drizzle(client as never, { logger } as never),
  };
}

async function createRedisRuntime(): Promise<PlaygroundRedisRuntime> {
  const Redis = await loadRedisConstructor();
  return {
    redis: new Redis() as unknown as PlaygroundRedisClient,
  };
}

async function loadRedisConstructor(): Promise<RedisConstructor> {
  redisCtorPromise ??= (async () => {
    const globalObject = globalThis as typeof globalThis & {
      process?: { versions?: { node?: unknown } };
      document?: unknown;
      window?: unknown;
    };
    const isNodeRuntime =
      typeof globalThis === "object" &&
      typeof globalObject.process === "object" &&
      typeof globalObject.process?.versions === "object" &&
      globalObject.process?.versions?.node != null;

    if (!isNodeRuntime) {
      return InMemoryRedisClient;
    }

    const nodeModule = await import("ioredis-mock");
    const nodeConstructor = (nodeModule as { default?: unknown }).default;
    if (typeof nodeConstructor === "function") {
      return nodeConstructor as RedisConstructor;
    }

    throw new Error("Failed to load the Node Redis mock constructor.");
  })();
  return redisCtorPromise;
}

async function loadPGliteConstructor(): Promise<PGliteConstructor> {
  pgliteCtorPromise ??= (async () => {
    const preferCdn =
      typeof globalThis === "object" &&
      typeof globalThis.location === "object" &&
      (globalThis.location.protocol === "http:" || globalThis.location.protocol === "https:");
    if (preferCdn) {
      try {
        const cdnModule = await import(/* @vite-ignore */ PGLITE_CDN_URL);
        const cdnConstructor = (cdnModule as { PGlite?: unknown }).PGlite;
        if (typeof cdnConstructor === "function") {
          return cdnConstructor as PGliteConstructor;
        }
      } catch {
        // fall through to local fallback
      }
    }

    const localModule = await import("@electric-sql/pglite");
    const localConstructor = (localModule as { PGlite?: unknown }).PGlite;
    if (typeof localConstructor === "function") {
      return localConstructor as PGliteConstructor;
    }

    throw new Error("Failed to load PGlite constructor.");
  })();
  return pgliteCtorPromise;
}

export async function getPlaygroundPgliteRuntime(): Promise<PlaygroundPgliteRuntime> {
  pgliteRuntimePromise ??= createPgliteRuntime();
  return pgliteRuntimePromise;
}

export async function getPlaygroundRedisRuntime(): Promise<PlaygroundRedisRuntime> {
  redisRuntimePromise ??= createRedisRuntime();
  return redisRuntimePromise;
}

export async function reseedDownstreamDatabase(rows: DownstreamRows): Promise<void> {
  const [dbRuntime, redisRuntime] = await Promise.all([
    getPlaygroundPgliteRuntime(),
    getPlaygroundRedisRuntime(),
  ]);
  await execStatements(dbRuntime.client, DROP_SCHEMA_STATEMENTS);
  await execStatements(dbRuntime.client, CREATE_SCHEMA_STATEMENTS);

  const orgRows = (rows.orgs ?? []) as QueryRow[];
  const userRows = (rows.users ?? []) as QueryRow[];
  const vendorRows = (rows.vendors ?? []) as QueryRow[];
  const productRows = (rows.products ?? []) as QueryRow[];
  const orderRows = (rows.orders ?? []) as QueryRow[];
  const itemRows = (rows.order_items ?? []) as QueryRow[];
  const accessRows = (rows.user_product_access ?? []) as QueryRow[];

  if (orgRows.length > 0) {
    await insertRowsSerial(dbRuntime.db, orgsTable, orgRows);
  }
  if (userRows.length > 0) {
    await insertRowsSerial(dbRuntime.db, usersTable, userRows);
  }
  if (vendorRows.length > 0) {
    await insertRowsSerial(dbRuntime.db, vendorsTable, vendorRows);
  }
  if (productRows.length > 0) {
    await insertRowsSerial(dbRuntime.db, productsTable, productRows);
  }
  if (orderRows.length > 0) {
    await insertRowsSerial(dbRuntime.db, ordersTable, orderRows);
  }
  if (itemRows.length > 0) {
    await insertRowsSerial(dbRuntime.db, orderItemsTable, itemRows);
  }
  if (accessRows.length > 0) {
    await insertRowsSerial(dbRuntime.db, userProductAccessTable, accessRows);
  }

  await seedRedisHashes(redisRuntime.redis, readRedisInputRows(rows));
}

export function clearExecutedProviderOperations(): void {
  executedProviderOperations.length = 0;
  nextOperationId = 1;
}

export function getExecutedProviderOperations(): ExecutedProviderOperation[] {
  return [...executedProviderOperations];
}
