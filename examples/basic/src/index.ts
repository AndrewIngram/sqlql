import {
  createArrayTableMethods,
  defineSchema,
  defineTableMethods,
  toSqlDDL,
  type QueryRow,
  query,
} from "sqlql";

async function main(): Promise<void> {
  const schema = defineSchema({
    tables: {
      orders: {
        columns: {
          id: "text",
          org_id: "text",
          user_id: "text",
          total_cents: "integer",
          created_at: "timestamp",
        },
      },
      users: {
        columns: {
          id: "text",
          team_id: "text",
          email: "text",
        },
      },
      teams: {
        columns: {
          id: "text",
          name: "text",
          tier: "text",
        },
      },
    },
  });

  const tableData = {
    orders: [
      {
        id: "ord_1",
        org_id: "org_1",
        user_id: "usr_1",
        total_cents: 1200,
        created_at: "2026-02-01",
      },
      {
        id: "ord_2",
        org_id: "org_1",
        user_id: "usr_1",
        total_cents: 1800,
        created_at: "2026-02-03",
      },
      {
        id: "ord_3",
        org_id: "org_1",
        user_id: "usr_2",
        total_cents: 2400,
        created_at: "2026-02-04",
      },
      {
        id: "ord_4",
        org_id: "org_2",
        user_id: "usr_3",
        total_cents: 9900,
        created_at: "2026-02-05",
      },
    ],
    users: [
      { id: "usr_1", team_id: "team_enterprise", email: "alice@example.com" },
      { id: "usr_2", team_id: "team_smb", email: "bob@example.com" },
      { id: "usr_3", team_id: "team_enterprise", email: "charlie@example.com" },
    ],
    teams: [
      { id: "team_enterprise", name: "Enterprise", tier: "enterprise" },
      { id: "team_smb", name: "SMB", tier: "smb" },
    ],
  } satisfies { orders: QueryRow[]; users: QueryRow[]; teams: QueryRow[] };

  const methods = defineTableMethods(schema, {
    orders: createArrayTableMethods(tableData.orders),
    users: createArrayTableMethods(tableData.users),
    teams: createArrayTableMethods(tableData.teams),
  });

  const ddl = toSqlDDL(schema, { ifNotExists: true });

  const basicRows = await query({
    schema,
    methods,
    context: {},
    sql: `
      SELECT o.id, o.total_cents
      FROM orders o
      WHERE o.org_id = 'org_1'
      ORDER BY o.created_at DESC
      LIMIT 2 OFFSET 1
    `,
  });

  const joinRows = await query({
    schema,
    methods,
    context: {},
    sql: `
      SELECT o.id, o.total_cents, u.email
      FROM orders o
      JOIN users u ON o.user_id = u.id
      WHERE o.org_id = 'org_1'
      ORDER BY o.created_at DESC
      LIMIT 3
    `,
  });

  const threeWayJoinRows = await query({
    schema,
    methods,
    context: {},
    sql: `
      SELECT o.id, u.email, t.name
      FROM orders o
      JOIN users u ON o.user_id = u.id
      JOIN teams t ON u.team_id = t.id
      WHERE o.org_id = 'org_1' AND t.tier = 'enterprise'
      ORDER BY o.created_at DESC
      LIMIT 10
    `,
  });

  const aggregateRows = await query({
    schema,
    methods,
    context: {},
    sql: `
      SELECT o.user_id, COUNT(*) AS order_count, SUM(o.total_cents) AS total_cents
      FROM orders o
      WHERE o.org_id = 'org_1'
      GROUP BY o.user_id
      ORDER BY total_cents DESC
    `,
  });

  const cteRows = await query({
    schema,
    methods,
    context: {},
    sql: `
      WITH recent_orders AS (
        SELECT id, user_id, total_cents
        FROM orders
        WHERE org_id = 'org_1' AND total_cents >= 1800
      )
      SELECT r.user_id, COUNT(*) AS recent_order_count, SUM(r.total_cents) AS total_cents
      FROM recent_orders r
      GROUP BY r.user_id
      ORDER BY total_cents DESC
    `,
  });

  console.log("Generated DDL:");
  console.log(ddl);
  console.log("");
  console.log("Basic query result:");
  console.log(basicRows);
  console.log("Join result:");
  console.log(joinRows);
  console.log("Three-way join result:");
  console.log(threeWayJoinRows);
  console.log("Aggregate result:");
  console.log(aggregateRows);
  console.log("CTE + aggregate result:");
  console.log(cteRows);
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
