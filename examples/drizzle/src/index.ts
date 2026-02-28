import Database from "better-sqlite3";
import { eq, inArray } from "drizzle-orm";
import { drizzle, type BetterSQLite3Database } from "drizzle-orm/better-sqlite3";
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";
import { createDrizzleTableMethods, impossibleCondition } from "@sqlql/drizzle";
import {
  defineSchema,
  defineTableMethods,
  query,
  toSqlDDL,
  type TableMethodsForSchema,
} from "sqlql";

const usersTable = sqliteTable("users", {
  id: text("id").primaryKey().notNull(),
  email: text("email").notNull(),
  displayName: text("display_name").notNull(),
});

const organizationsTable = sqliteTable("organizations", {
  id: text("id").primaryKey().notNull(),
  name: text("name").notNull(),
});

const organizationMembershipsTable = sqliteTable("organization_memberships", {
  orgId: text("org_id").notNull(),
  userId: text("user_id").notNull(),
  role: text("role").notNull(),
});

const projectsTable = sqliteTable("projects", {
  id: text("id").primaryKey().notNull(),
  orgId: text("org_id").notNull(),
  name: text("name").notNull(),
  status: text("status").notNull(),
  ownerUserId: text("owner_user_id").notNull(),
  budgetCents: integer("budget_cents").notNull(),
});

const usageEventsTable = sqliteTable("usage_events", {
  id: text("id").primaryKey().notNull(),
  orgId: text("org_id").notNull(),
  projectId: text("project_id").notNull(),
  userId: text("user_id").notNull(),
  eventType: text("event_type").notNull(),
  creditsUsed: integer("credits_used").notNull(),
  occurredAt: text("occurred_at").notNull(),
});

const invoicesTable = sqliteTable("invoices", {
  id: text("id").primaryKey().notNull(),
  orgId: text("org_id").notNull(),
  status: text("status").notNull(),
  currency: text("currency").notNull(),
  amountDue: integer("amount_due").notNull(),
  amountPaid: integer("amount_paid").notNull(),
  amountOverpaid: integer("amount_overpaid").notNull(),
  amountRemaining: integer("amount_remaining").notNull(),
  amountShipping: integer("amount_shipping"),
  issuedAt: text("issued_at").notNull(),
});

const adminNotesTable = sqliteTable("admin_notes", {
  id: text("id").primaryKey().notNull(),
  orgId: text("org_id").notNull(),
  note: text("note").notNull(),
});

const schema = defineSchema({
  tables: {
    users: {
      columns: {
        id: { type: "text", nullable: false },
        email: { type: "text", nullable: false },
        display_name: { type: "text", nullable: false },
      },
      constraints: {
        primaryKey: {
          columns: ["id"],
        },
        unique: [
          {
            columns: ["email"],
          },
        ],
      },
    },
    projects: {
      columns: {
        id: { type: "text", nullable: false },
        name: { type: "text", nullable: false },
        status: { type: "text", nullable: false },
        owner_user_id: { type: "text", nullable: false },
        budget_cents: { type: "integer", nullable: false },
      },
      constraints: {
        primaryKey: {
          columns: ["id"],
        },
        foreignKeys: [
          {
            columns: ["owner_user_id"],
            references: {
              table: "users",
              columns: ["id"],
            },
          },
        ],
      },
    },
    usage_events: {
      columns: {
        id: { type: "text", nullable: false },
        project_id: { type: "text", nullable: false },
        user_id: { type: "text", nullable: false },
        event_type: { type: "text", nullable: false },
        credits_used: { type: "integer", nullable: false },
        occurred_at: { type: "timestamp", nullable: false },
      },
      constraints: {
        primaryKey: {
          columns: ["id"],
        },
        foreignKeys: [
          {
            columns: ["project_id"],
            references: {
              table: "projects",
              columns: ["id"],
            },
          },
          {
            columns: ["user_id"],
            references: {
              table: "users",
              columns: ["id"],
            },
          },
        ],
      },
    },
    invoices: {
      columns: {
        id: { type: "text", nullable: false },
        status: { type: "text", nullable: false },
        currency: { type: "text", nullable: false },
        amount_due: { type: "integer", nullable: false },
        amount_paid: { type: "integer", nullable: false },
        amount_overpaid: { type: "integer", nullable: false },
        amount_remaining: { type: "integer", nullable: false },
        amount_shipping: { type: "integer", nullable: true },
        issued_at: { type: "timestamp", nullable: false },
      },
      constraints: {
        primaryKey: {
          columns: ["id"],
        },
      },
    },
  },
});

type AppContext = {
  userId: string;
};

const userColumns = {
  id: usersTable.id,
  email: usersTable.email,
  display_name: usersTable.displayName,
} as const;

const projectColumns = {
  id: projectsTable.id,
  name: projectsTable.name,
  status: projectsTable.status,
  owner_user_id: projectsTable.ownerUserId,
  budget_cents: projectsTable.budgetCents,
} as const;

const usageEventColumns = {
  id: usageEventsTable.id,
  project_id: usageEventsTable.projectId,
  user_id: usageEventsTable.userId,
  event_type: usageEventsTable.eventType,
  credits_used: usageEventsTable.creditsUsed,
  occurred_at: usageEventsTable.occurredAt,
} as const;

const invoiceColumns = {
  id: invoicesTable.id,
  status: invoicesTable.status,
  currency: invoicesTable.currency,
  amount_due: invoicesTable.amountDue,
  amount_paid: invoicesTable.amountPaid,
  amount_overpaid: invoicesTable.amountOverpaid,
  amount_remaining: invoicesTable.amountRemaining,
  amount_shipping: invoicesTable.amountShipping,
  issued_at: invoicesTable.issuedAt,
} as const;

async function main(): Promise<void> {
  const sqlite = new Database(":memory:");
  const db = drizzle(sqlite);

  seedDatabase(sqlite, db);

  const methodMap: TableMethodsForSchema<typeof schema, AppContext> = {
    users: createDrizzleTableMethods({
      db,
      tableName: "users",
      table: usersTable,
      columns: userColumns,
      scope: (context) => {
        const orgIds = readScopedOrgIds(db, context.userId);
        if (orgIds.length === 0) {
          return impossibleCondition();
        }

        const scopedUserIds = readScopedUserIds(db, orgIds);
        if (scopedUserIds.length === 0) {
          return impossibleCondition();
        }

        return inArray(usersTable.id, scopedUserIds);
      },
    }),
    projects: createDrizzleTableMethods({
      db,
      tableName: "projects",
      table: projectsTable,
      columns: projectColumns,
      scope: (context) => {
        const orgIds = readScopedOrgIds(db, context.userId);
        return orgIds.length > 0 ? inArray(projectsTable.orgId, orgIds) : impossibleCondition();
      },
    }),
    usage_events: createDrizzleTableMethods({
      db,
      tableName: "usage_events",
      table: usageEventsTable,
      columns: usageEventColumns,
      scope: (context) => {
        const orgIds = readScopedOrgIds(db, context.userId);
        return orgIds.length > 0 ? inArray(usageEventsTable.orgId, orgIds) : impossibleCondition();
      },
    }),
    invoices: createDrizzleTableMethods({
      db,
      tableName: "invoices",
      table: invoicesTable,
      columns: invoiceColumns,
      scope: (context) => {
        const orgIds = readScopedOrgIds(db, context.userId);
        return orgIds.length > 0 ? inArray(invoicesTable.orgId, orgIds) : impossibleCondition();
      },
    }),
  };

  const methods = defineTableMethods(schema, methodMap);

  console.log("Facade DDL with PK/FK/Unique metadata:");
  console.log(toSqlDDL(schema, { ifNotExists: true }));
  console.log("");

  const usageSummarySql = `
    WITH active_usage AS (
      SELECT p.id AS project_id, p.name AS project_name, p.owner_user_id, e.credits_used
      FROM projects p
      JOIN usage_events e ON e.project_id = p.id
      WHERE p.status = 'active'
    )
    SELECT au.project_name, u.display_name AS owner, SUM(au.credits_used) AS total_credits
    FROM active_usage au
    JOIN users u ON u.id = au.owner_user_id
    GROUP BY au.project_name, u.display_name
    ORDER BY total_credits DESC
  `;

  const invoiceSql = `
    SELECT status, currency, amount_due, amount_paid, amount_overpaid, amount_remaining, amount_shipping
    FROM invoices
    ORDER BY issued_at DESC
    LIMIT 20
  `;

  for (const userId of ["usr_alice", "usr_bob"]) {
    console.log(`Results for context userId=${userId}`);
    const usageRows = await query({
      schema,
      methods,
      context: { userId },
      sql: usageSummarySql,
    });
    const invoiceRows = await query({
      schema,
      methods,
      context: { userId },
      sql: invoiceSql,
    });

    console.log("Usage summary:");
    console.log(usageRows);
    console.log("Invoice summary:");
    console.log(invoiceRows);
    console.log("");
  }

  try {
    await query({
      schema,
      methods,
      context: {
        userId: "usr_alice",
      },
      sql: "SELECT id, note FROM admin_notes",
    });
  } catch (error) {
    console.log("As expected, internal-only table access is rejected:");
    console.log((error as Error).message);
  }
}

function readScopedOrgIds(db: BetterSQLite3Database, userId: string): string[] {
  const rows = db
    .select({
      org_id: organizationMembershipsTable.orgId,
    })
    .from(organizationMembershipsTable)
    .where(eq(organizationMembershipsTable.userId, userId))
    .all();

  return dedupe(rows.map((row) => row.org_id));
}

function readScopedUserIds(db: BetterSQLite3Database, orgIds: string[]): string[] {
  const rows = db
    .select({
      user_id: organizationMembershipsTable.userId,
    })
    .from(organizationMembershipsTable)
    .where(inArray(organizationMembershipsTable.orgId, orgIds))
    .all();

  return dedupe(rows.map((row) => row.user_id));
}

function dedupe(values: string[]): string[] {
  return [...new Set(values)];
}

function seedDatabase(sqlite: InstanceType<typeof Database>, db: BetterSQLite3Database): void {
  sqlite.exec("PRAGMA foreign_keys = ON;");
  sqlite.exec(`
    CREATE TABLE organizations (
      id TEXT PRIMARY KEY NOT NULL,
      name TEXT NOT NULL
    );

    CREATE TABLE users (
      id TEXT PRIMARY KEY NOT NULL,
      email TEXT NOT NULL UNIQUE,
      display_name TEXT NOT NULL
    );

    CREATE TABLE organization_memberships (
      org_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      role TEXT NOT NULL,
      FOREIGN KEY (org_id) REFERENCES organizations(id),
      FOREIGN KEY (user_id) REFERENCES users(id)
    );

    CREATE TABLE projects (
      id TEXT PRIMARY KEY NOT NULL,
      org_id TEXT NOT NULL,
      name TEXT NOT NULL,
      status TEXT NOT NULL,
      owner_user_id TEXT NOT NULL,
      budget_cents INTEGER NOT NULL,
      FOREIGN KEY (org_id) REFERENCES organizations(id),
      FOREIGN KEY (owner_user_id) REFERENCES users(id)
    );

    CREATE TABLE usage_events (
      id TEXT PRIMARY KEY NOT NULL,
      org_id TEXT NOT NULL,
      project_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      event_type TEXT NOT NULL,
      credits_used INTEGER NOT NULL,
      occurred_at TEXT NOT NULL,
      FOREIGN KEY (org_id) REFERENCES organizations(id),
      FOREIGN KEY (project_id) REFERENCES projects(id),
      FOREIGN KEY (user_id) REFERENCES users(id)
    );

    CREATE TABLE invoices (
      id TEXT PRIMARY KEY NOT NULL,
      org_id TEXT NOT NULL,
      status TEXT NOT NULL,
      currency TEXT NOT NULL,
      amount_due INTEGER NOT NULL,
      amount_paid INTEGER NOT NULL,
      amount_overpaid INTEGER NOT NULL,
      amount_remaining INTEGER NOT NULL,
      amount_shipping INTEGER,
      issued_at TEXT NOT NULL,
      FOREIGN KEY (org_id) REFERENCES organizations(id)
    );

    CREATE TABLE admin_notes (
      id TEXT PRIMARY KEY NOT NULL,
      org_id TEXT NOT NULL,
      note TEXT NOT NULL,
      FOREIGN KEY (org_id) REFERENCES organizations(id)
    );
  `);

  db.insert(organizationsTable)
    .values([
      { id: "org_acme", name: "Acme Analytics" },
      { id: "org_beta", name: "Beta Logistics" },
    ])
    .run();

  db.insert(usersTable)
    .values([
      { id: "usr_alice", email: "alice@acme.test", displayName: "Alice" },
      { id: "usr_ava", email: "ava@acme.test", displayName: "Ava" },
      { id: "usr_bob", email: "bob@beta.test", displayName: "Bob" },
    ])
    .run();

  db.insert(organizationMembershipsTable)
    .values([
      { orgId: "org_acme", userId: "usr_alice", role: "owner" },
      { orgId: "org_acme", userId: "usr_ava", role: "member" },
      { orgId: "org_beta", userId: "usr_bob", role: "owner" },
    ])
    .run();

  db.insert(projectsTable)
    .values([
      {
        id: "prj_acme_core",
        orgId: "org_acme",
        name: "Core Platform",
        status: "active",
        ownerUserId: "usr_alice",
        budgetCents: 120000,
      },
      {
        id: "prj_acme_growth",
        orgId: "org_acme",
        name: "Growth Pipeline",
        status: "paused",
        ownerUserId: "usr_ava",
        budgetCents: 80000,
      },
      {
        id: "prj_beta_mobile",
        orgId: "org_beta",
        name: "Mobile Rollout",
        status: "active",
        ownerUserId: "usr_bob",
        budgetCents: 50000,
      },
    ])
    .run();

  db.insert(usageEventsTable)
    .values([
      {
        id: "evt_1",
        orgId: "org_acme",
        projectId: "prj_acme_core",
        userId: "usr_alice",
        eventType: "build_minutes",
        creditsUsed: 120,
        occurredAt: "2026-02-01T10:00:00Z",
      },
      {
        id: "evt_2",
        orgId: "org_acme",
        projectId: "prj_acme_core",
        userId: "usr_ava",
        eventType: "build_minutes",
        creditsUsed: 80,
        occurredAt: "2026-02-02T10:00:00Z",
      },
      {
        id: "evt_3",
        orgId: "org_beta",
        projectId: "prj_beta_mobile",
        userId: "usr_bob",
        eventType: "build_minutes",
        creditsUsed: 45,
        occurredAt: "2026-02-01T11:00:00Z",
      },
    ])
    .run();

  db.insert(invoicesTable)
    .values([
      {
        id: "inv_1",
        orgId: "org_acme",
        status: "open",
        currency: "usd",
        amountDue: 30000,
        amountPaid: 10000,
        amountOverpaid: 0,
        amountRemaining: 20000,
        amountShipping: 0,
        issuedAt: "2026-02-03T00:00:00Z",
      },
      {
        id: "inv_2",
        orgId: "org_acme",
        status: "paid",
        currency: "usd",
        amountDue: 12000,
        amountPaid: 12000,
        amountOverpaid: 0,
        amountRemaining: 0,
        amountShipping: 0,
        issuedAt: "2026-01-15T00:00:00Z",
      },
      {
        id: "inv_3",
        orgId: "org_beta",
        status: "open",
        currency: "eur",
        amountDue: 7000,
        amountPaid: 2000,
        amountOverpaid: 0,
        amountRemaining: 5000,
        amountShipping: 200,
        issuedAt: "2026-02-02T00:00:00Z",
      },
    ])
    .run();

  db.insert(adminNotesTable)
    .values([
      {
        id: "note_1",
        orgId: "org_acme",
        note: "Escalate renewal in Q3.",
      },
      {
        id: "note_2",
        orgId: "org_beta",
        note: "Finance review pending.",
      },
    ])
    .run();
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
