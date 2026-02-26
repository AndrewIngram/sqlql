import {
  defineSchema,
  defineTableMethods,
  toSqlDDL,
  type QueryRow,
  type TableScanRequest,
  query,
} from "sqlql";

function runScan(rows: QueryRow[], request: TableScanRequest): QueryRow[] {
  let out = [...rows];

  for (const clause of request.where ?? []) {
    if (clause.op === "eq") {
      out = out.filter((row) => row[clause.column] === clause.value);
    } else if (clause.op === "in") {
      const set = new Set(clause.values);
      out = out.filter((row) => set.has(row[clause.column]));
    }
  }

  if (request.orderBy) {
    out.sort((left, right) => {
      for (const term of request.orderBy ?? []) {
        const leftValue = left[term.column] as string | number;
        const rightValue = right[term.column] as string | number;
        if (leftValue === rightValue) {
          continue;
        }
        const comparison = leftValue < rightValue ? -1 : 1;
        return term.direction === "asc" ? comparison : -comparison;
      }
      return 0;
    });
  }

  if (request.limit != null) {
    out = out.slice(0, request.limit);
  }

  return out.map((row) =>
    Object.fromEntries(request.select.map((column) => [column, row[column] ?? null])),
  );
}

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

  const methods = defineTableMethods({
    orders: {
      async scan(request) {
        return runScan(tableData.orders, request);
      },
    },
    users: {
      async scan(request) {
        return runScan(tableData.users, request);
      },
    },
    teams: {
      async scan(request) {
        return runScan(tableData.teams, request);
      },
    },
  });

  const ddl = toSqlDDL(schema, { ifNotExists: true });

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

  console.log("Generated DDL:");
  console.log(ddl);
  console.log("");
  console.log("Join result:");
  console.log(joinRows);
  console.log("Three-way join result:");
  console.log(threeWayJoinRows);
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
