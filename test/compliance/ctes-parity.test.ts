import { commerceRows, commerceSchema } from "../support/commerce-fixture";
import { registerParityCases, type ComplianceCase } from "./support/case-runner";

const cases: ComplianceCase[] = [
  {
    name: "single CTE",
    sql: `
      WITH scoped AS (
        SELECT id, user_id
        FROM orders
        WHERE org_id = 'org_1'
      )
      SELECT user_id, COUNT(*) AS n
      FROM scoped
      GROUP BY user_id
      ORDER BY user_id ASC
    `,
    expectedRows: [
      { user_id: "usr_1", n: 2 },
      { user_id: "usr_2", n: 1 },
    ],
  },
  {
    name: "chained CTEs",
    sql: `
      WITH scoped AS (
        SELECT user_id, total_cents
        FROM orders
        WHERE org_id = 'org_1'
      ),
      totals AS (
        SELECT user_id, SUM(total_cents) AS total_cents
        FROM scoped
        GROUP BY user_id
      )
      SELECT user_id
      FROM totals
      WHERE total_cents >= 2500
      ORDER BY user_id ASC
    `,
    expectedRows: [{ user_id: "usr_1" }],
  },
];

registerParityCases(
  "compliance/ctes-parity",
  {
    schema: commerceSchema,
    rowsByTable: commerceRows,
  },
  cases,
);
