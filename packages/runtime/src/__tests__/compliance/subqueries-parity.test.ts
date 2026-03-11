import { commerceRows, commerceSchema } from "@tupl/test-support/fixtures";
import { registerParityCases, type ComplianceCase } from "@tupl/test-support/runtime";

const cases: ComplianceCase[] = [
  {
    name: "IN subquery",
    sql: `
      SELECT id
      FROM orders
      WHERE user_id IN (
        SELECT id
        FROM users
        WHERE team_id = 'team_enterprise'
      )
      ORDER BY id ASC
    `,
    expectedRows: [{ id: "ord_1" }, { id: "ord_2" }, { id: "ord_4" }],
  },
  {
    name: "EXISTS subquery",
    sql: `
      SELECT id
      FROM orders o
      WHERE EXISTS (
        SELECT id
        FROM users
        WHERE team_id = 'team_smb'
      )
      ORDER BY id ASC
    `,
    expectedRows: [{ id: "ord_1" }, { id: "ord_2" }, { id: "ord_3" }, { id: "ord_4" }],
  },
  {
    name: "scalar subquery in WHERE",
    sql: `
      SELECT id
      FROM orders
      WHERE total_cents = (SELECT MAX(total_cents) FROM orders)
    `,
    expectedRows: [{ id: "ord_4" }],
  },
  {
    name: "scalar subquery in SELECT",
    sql: `
      SELECT id, (SELECT MAX(total_cents) FROM orders) AS max_total
      FROM orders
      ORDER BY id ASC
      LIMIT 2
    `,
    expectedRows: [
      { id: "ord_1", max_total: 9900 },
      { id: "ord_2", max_total: 9900 },
    ],
  },
];

registerParityCases(
  "compliance/subqueries-parity",
  {
    schema: commerceSchema,
    rowsByTable: commerceRows,
  },
  cases,
);
