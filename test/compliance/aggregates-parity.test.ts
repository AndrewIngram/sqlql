import { commerceRows, commerceSchema } from "../support/commerce-fixture";
import { registerParityCases, type ComplianceCase } from "./support/case-runner";

const cases: ComplianceCase[] = [
  {
    name: "GROUP BY with COUNT and SUM",
    sql: `
      SELECT user_id, COUNT(*) AS order_count, SUM(total_cents) AS total_cents
      FROM orders
      WHERE org_id = 'org_1'
      GROUP BY user_id
      ORDER BY user_id ASC
    `,
    expectedRows: [
      { user_id: "usr_1", order_count: 2, total_cents: 3000 },
      { user_id: "usr_2", order_count: 1, total_cents: 2400 },
    ],
  },
  {
    name: "COUNT(DISTINCT column)",
    sql: `
      SELECT COUNT(DISTINCT user_id) AS distinct_users
      FROM orders
      WHERE org_id = 'org_1'
    `,
    expectedRows: [{ distinct_users: 2 }],
  },
  {
    name: "HAVING aggregate predicate",
    sql: `
      SELECT user_id, COUNT(*) AS order_count
      FROM orders
      GROUP BY user_id
      HAVING COUNT(*) >= 2
      ORDER BY user_id ASC
    `,
    expectedRows: [{ user_id: "usr_1", order_count: 2 }],
  },
  {
    name: "aggregate over empty input",
    sql: `
      SELECT COUNT(*) AS n, SUM(total_cents) AS sum_total
      FROM orders
      WHERE org_id = 'org_missing'
    `,
    expectedRows: [{ n: 0, sum_total: null }],
  },
];

registerParityCases(
  "compliance/aggregates-parity",
  {
    schema: commerceSchema,
    rowsByTable: commerceRows,
  },
  cases,
);
