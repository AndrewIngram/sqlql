import Database from "better-sqlite3";

export type DemoContext = {
  orgId: string;
  userId: string;
};

export const SQLITE_DDL = `
  CREATE TABLE vendors_raw (
    id TEXT PRIMARY KEY NOT NULL,
    org_id TEXT NOT NULL,
    name TEXT NOT NULL
  );

  CREATE TABLE orders_raw (
    id TEXT PRIMARY KEY NOT NULL,
    org_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    vendor_id TEXT NOT NULL,
    total_cents INTEGER NOT NULL,
    created_at TEXT NOT NULL
  );
`;

export const SQLITE_SEED = `
  INSERT INTO vendors_raw (id, org_id, name) VALUES
    ('v1', 'org_1', 'Northwind'),
    ('v2', 'org_1', 'Acme Parts'),
    ('v3', 'org_2', 'Other Org Vendor');

  INSERT INTO orders_raw (id, org_id, user_id, vendor_id, total_cents, created_at) VALUES
    ('o1', 'org_1', 'u1', 'v1', 1500, '2026-02-01T00:00:00.000Z'),
    ('o2', 'org_1', 'u1', 'v2', 3200, '2026-02-03T00:00:00.000Z'),
    ('o3', 'org_1', 'u2', 'v1', 7000, '2026-02-04T00:00:00.000Z'),
    ('o4', 'org_2', 'u9', 'v3', 1200, '2026-02-05T00:00:00.000Z');
`;

export function createSeededSqliteDatabase(): Database.Database {
  const sqlite = new Database(":memory:");
  sqlite.exec(SQLITE_DDL);
  sqlite.exec(SQLITE_SEED);
  return sqlite;
}

export function closeSqliteDatabase(db: Database.Database): void {
  db.close();
}
