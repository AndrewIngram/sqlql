import type { CatalogQueryEntry, ExamplePack } from "./types";

const commerce: ExamplePack = {
  id: "commerce",
  label: "Commerce",
  description: "Orders + catalog data with joins, aggregates, CTEs, and windows.",
  schema: {
    tables: {
      customers: {
        columns: {
          id: { type: "text", nullable: false },
          full_name: { type: "text", nullable: false },
          region: { type: "text", nullable: false },
        },
        constraints: {
          primaryKey: { columns: ["id"] },
        },
      },
      products: {
        columns: {
          id: { type: "text", nullable: false },
          sku: { type: "text", nullable: false },
          name: { type: "text", nullable: false },
          category: { type: "text", nullable: false },
        },
        constraints: {
          primaryKey: { columns: ["id"] },
          unique: [{ columns: ["sku"] }],
        },
      },
      orders: {
        columns: {
          id: { type: "text", nullable: false },
          customer_id: { type: "text", nullable: false },
          status: { type: "text", nullable: false },
          total_cents: { type: "integer", nullable: false },
          ordered_at: { type: "timestamp", nullable: false },
        },
        constraints: {
          primaryKey: { columns: ["id"] },
          foreignKeys: [
            {
              columns: ["customer_id"],
              references: {
                table: "customers",
                columns: ["id"],
              },
            },
          ],
        },
      },
      order_items: {
        columns: {
          id: { type: "text", nullable: false },
          order_id: { type: "text", nullable: false },
          product_id: { type: "text", nullable: false },
          quantity: { type: "integer", nullable: false },
          line_total_cents: { type: "integer", nullable: false },
        },
        constraints: {
          primaryKey: { columns: ["id"] },
          foreignKeys: [
            {
              columns: ["order_id"],
              references: {
                table: "orders",
                columns: ["id"],
              },
            },
            {
              columns: ["product_id"],
              references: {
                table: "products",
                columns: ["id"],
              },
            },
          ],
        },
      },
    },
  },
  rows: {
    customers: [
      { id: "cust_1", full_name: "Maya Ortiz", region: "us-east" },
      { id: "cust_2", full_name: "Noah Singh", region: "eu-west" },
      { id: "cust_3", full_name: "Liam Chen", region: "us-east" },
    ],
    products: [
      { id: "prod_1", sku: "watch-001", name: "Running Watch", category: "wearables" },
      { id: "prod_2", sku: "shoe-002", name: "Tempo Shoes", category: "footwear" },
      { id: "prod_3", sku: "band-003", name: "Heart Band", category: "wearables" },
    ],
    orders: [
      {
        id: "ord_1",
        customer_id: "cust_1",
        status: "paid",
        total_cents: 15900,
        ordered_at: "2026-02-01T10:00:00Z",
      },
      {
        id: "ord_2",
        customer_id: "cust_1",
        status: "paid",
        total_cents: 9900,
        ordered_at: "2026-02-04T08:00:00Z",
      },
      {
        id: "ord_3",
        customer_id: "cust_2",
        status: "pending",
        total_cents: 4200,
        ordered_at: "2026-02-05T09:30:00Z",
      },
      {
        id: "ord_4",
        customer_id: "cust_3",
        status: "paid",
        total_cents: 25900,
        ordered_at: "2026-02-07T11:20:00Z",
      },
    ],
    order_items: [
      {
        id: "item_1",
        order_id: "ord_1",
        product_id: "prod_1",
        quantity: 1,
        line_total_cents: 12900,
      },
      {
        id: "item_2",
        order_id: "ord_1",
        product_id: "prod_3",
        quantity: 1,
        line_total_cents: 3000,
      },
      {
        id: "item_3",
        order_id: "ord_2",
        product_id: "prod_2",
        quantity: 1,
        line_total_cents: 9900,
      },
      {
        id: "item_4",
        order_id: "ord_4",
        product_id: "prod_1",
        quantity: 2,
        line_total_cents: 25800,
      },
    ],
  },
  queries: [
    {
      label: "Join orders + customers",
      sql: `
SELECT o.id, c.full_name, o.total_cents
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.status = 'paid'
ORDER BY o.ordered_at DESC
LIMIT 10;
      `.trim(),
    },
    {
      label: "CTE + aggregate",
      sql: `
WITH paid_orders AS (
  SELECT customer_id, total_cents
  FROM orders
  WHERE status = 'paid'
)
SELECT c.full_name, COUNT(*) AS order_count, SUM(p.total_cents) AS gross_cents
FROM paid_orders p
JOIN customers c ON p.customer_id = c.id
GROUP BY c.full_name
ORDER BY gross_cents DESC;
      `.trim(),
    },
    {
      label: "Window ranking",
      sql: `
SELECT
  o.id,
  o.customer_id,
  o.total_cents,
  RANK() OVER (PARTITION BY o.customer_id ORDER BY o.total_cents DESC) AS spend_rank
FROM orders o
ORDER BY o.customer_id, spend_rank;
      `.trim(),
    },
  ],
};

const finance: ExamplePack = {
  id: "finance",
  label: "Finance",
  description: "Accounts + transactions for balance and spending analytics.",
  schema: {
    tables: {
      accounts: {
        columns: {
          id: { type: "text", nullable: false },
          owner_name: { type: "text", nullable: false },
          account_type: { type: "text", nullable: false },
        },
        constraints: {
          primaryKey: { columns: ["id"] },
        },
      },
      transactions: {
        columns: {
          id: { type: "text", nullable: false },
          account_id: { type: "text", nullable: false },
          posted_at: { type: "timestamp", nullable: false },
          kind: { type: "text", nullable: false },
          amount_cents: { type: "integer", nullable: false },
          merchant: { type: "text", nullable: true },
        },
        constraints: {
          primaryKey: { columns: ["id"] },
          foreignKeys: [
            {
              columns: ["account_id"],
              references: {
                table: "accounts",
                columns: ["id"],
              },
            },
          ],
        },
      },
      monthly_budgets: {
        columns: {
          id: { type: "text", nullable: false },
          account_id: { type: "text", nullable: false },
          month: { type: "text", nullable: false },
          category: { type: "text", nullable: false },
          limit_cents: { type: "integer", nullable: false },
        },
        constraints: {
          primaryKey: { columns: ["id"] },
          foreignKeys: [
            {
              columns: ["account_id"],
              references: {
                table: "accounts",
                columns: ["id"],
              },
            },
          ],
        },
      },
    },
  },
  rows: {
    accounts: [
      { id: "acct_1", owner_name: "Alex Kim", account_type: "checking" },
      { id: "acct_2", owner_name: "Alex Kim", account_type: "savings" },
    ],
    transactions: [
      {
        id: "txn_1",
        account_id: "acct_1",
        posted_at: "2026-02-01T08:00:00Z",
        kind: "expense",
        amount_cents: 5400,
        merchant: "Grocerio",
      },
      {
        id: "txn_2",
        account_id: "acct_1",
        posted_at: "2026-02-02T08:00:00Z",
        kind: "expense",
        amount_cents: 2200,
        merchant: "Coffee Lab",
      },
      {
        id: "txn_3",
        account_id: "acct_1",
        posted_at: "2026-02-03T08:00:00Z",
        kind: "income",
        amount_cents: 150000,
        merchant: null,
      },
      {
        id: "txn_4",
        account_id: "acct_2",
        posted_at: "2026-02-04T08:00:00Z",
        kind: "income",
        amount_cents: 20000,
        merchant: null,
      },
    ],
    monthly_budgets: [
      {
        id: "bud_1",
        account_id: "acct_1",
        month: "2026-02",
        category: "food",
        limit_cents: 50000,
      },
      {
        id: "bud_2",
        account_id: "acct_1",
        month: "2026-02",
        category: "coffee",
        limit_cents: 12000,
      },
    ],
  },
  queries: [
    {
      label: "Income vs expense aggregate",
      sql: `
SELECT account_id, kind, SUM(amount_cents) AS total_cents
FROM transactions
GROUP BY account_id, kind
ORDER BY account_id, kind;
      `.trim(),
    },
    {
      label: "CTE spend by merchant",
      sql: `
WITH expense_txns AS (
  SELECT account_id, merchant, amount_cents
  FROM transactions
  WHERE kind = 'expense'
)
SELECT a.owner_name, e.merchant, SUM(e.amount_cents) AS spend_cents
FROM expense_txns e
JOIN accounts a ON e.account_id = a.id
GROUP BY a.owner_name, e.merchant
ORDER BY spend_cents DESC;
      `.trim(),
    },
    {
      label: "Running income per account",
      sql: `
SELECT
  t.account_id,
  t.posted_at,
  t.amount_cents,
  SUM(t.amount_cents) OVER (PARTITION BY t.account_id ORDER BY t.posted_at) AS running_total
FROM transactions t
WHERE t.kind = 'income'
ORDER BY t.account_id, t.posted_at;
      `.trim(),
    },
  ],
};

const fitness: ExamplePack = {
  id: "fitness",
  label: "Fitness",
  description: "Athletes, workouts, and runs for coaching dashboards.",
  schema: {
    tables: {
      athletes: {
        columns: {
          id: { type: "text", nullable: false },
          display_name: { type: "text", nullable: false },
          level: { type: "text", nullable: false },
        },
        constraints: {
          primaryKey: { columns: ["id"] },
        },
      },
      workouts: {
        columns: {
          id: { type: "text", nullable: false },
          athlete_id: { type: "text", nullable: false },
          workout_type: { type: "text", nullable: false },
          duration_min: { type: "integer", nullable: false },
          completed_at: { type: "timestamp", nullable: false },
        },
        constraints: {
          primaryKey: { columns: ["id"] },
          foreignKeys: [
            {
              columns: ["athlete_id"],
              references: {
                table: "athletes",
                columns: ["id"],
              },
            },
          ],
        },
      },
      runs: {
        columns: {
          id: { type: "text", nullable: false },
          athlete_id: { type: "text", nullable: false },
          run_date: { type: "timestamp", nullable: false },
          distance_km: { type: "integer", nullable: false },
          pace_sec: { type: "integer", nullable: false },
        },
        constraints: {
          primaryKey: { columns: ["id"] },
          foreignKeys: [
            {
              columns: ["athlete_id"],
              references: {
                table: "athletes",
                columns: ["id"],
              },
            },
          ],
        },
      },
    },
  },
  rows: {
    athletes: [
      { id: "ath_1", display_name: "Sam", level: "beginner" },
      { id: "ath_2", display_name: "Priya", level: "intermediate" },
      { id: "ath_3", display_name: "Owen", level: "advanced" },
    ],
    workouts: [
      {
        id: "wo_1",
        athlete_id: "ath_1",
        workout_type: "strength",
        duration_min: 30,
        completed_at: "2026-02-01T07:00:00Z",
      },
      {
        id: "wo_2",
        athlete_id: "ath_2",
        workout_type: "run",
        duration_min: 45,
        completed_at: "2026-02-02T07:00:00Z",
      },
      {
        id: "wo_3",
        athlete_id: "ath_2",
        workout_type: "bike",
        duration_min: 55,
        completed_at: "2026-02-03T07:00:00Z",
      },
      {
        id: "wo_4",
        athlete_id: "ath_3",
        workout_type: "run",
        duration_min: 65,
        completed_at: "2026-02-04T07:00:00Z",
      },
    ],
    runs: [
      { id: "run_1", athlete_id: "ath_2", run_date: "2026-02-05", distance_km: 5, pace_sec: 315 },
      { id: "run_2", athlete_id: "ath_2", run_date: "2026-02-10", distance_km: 8, pace_sec: 340 },
      { id: "run_3", athlete_id: "ath_3", run_date: "2026-02-03", distance_km: 10, pace_sec: 285 },
      { id: "run_4", athlete_id: "ath_3", run_date: "2026-02-09", distance_km: 12, pace_sec: 295 },
    ],
  },
  queries: [
    {
      label: "Join workouts + athletes",
      sql: `
SELECT w.id, a.display_name, w.workout_type, w.duration_min
FROM workouts w
JOIN athletes a ON w.athlete_id = a.id
ORDER BY w.completed_at DESC
LIMIT 12;
      `.trim(),
    },
    {
      label: "CTE total workout minutes",
      sql: `
WITH recent_workouts AS (
  SELECT athlete_id, duration_min
  FROM workouts
  WHERE completed_at >= '2026-02-01'
)
SELECT a.display_name, SUM(r.duration_min) AS total_minutes
FROM recent_workouts r
JOIN athletes a ON r.athlete_id = a.id
GROUP BY a.display_name
ORDER BY total_minutes DESC;
      `.trim(),
    },
    {
      label: "Pace rank by athlete",
      sql: `
SELECT
  r.athlete_id,
  r.run_date,
  r.pace_sec,
  DENSE_RANK() OVER (PARTITION BY r.athlete_id ORDER BY r.pace_sec ASC) AS pace_rank
FROM runs r
ORDER BY r.athlete_id, pace_rank;
      `.trim(),
    },
  ],
};

export const EXAMPLE_PACKS: ExamplePack[] = [commerce, finance, fitness];

export function buildQueryCatalog(packs: ExamplePack[]): CatalogQueryEntry[] {
  const entries: CatalogQueryEntry[] = [];

  for (const pack of packs) {
    for (const [index, query] of pack.queries.entries()) {
      entries.push({
        id: `${pack.id}:${index}`,
        packId: pack.id,
        packLabel: pack.label,
        queryLabel: query.label,
        sql: query.sql,
      });
    }
  }

  return entries;
}

export function serializeJson(value: unknown): string {
  return `${JSON.stringify(value, null, 2)}\n`;
}
