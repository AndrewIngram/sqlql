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
    name: "correlated EXISTS subquery",
    sql: `
      SELECT o.id
      FROM orders o
      WHERE EXISTS (
        SELECT u.id
        FROM users u
        WHERE u.id = o.user_id
          AND u.team_id = 'team_smb'
      )
      ORDER BY o.id ASC
    `,
    expectedRows: [{ id: "ord_3" }],
  },
  {
    name: "correlated NOT EXISTS subquery",
    sql: `
      SELECT o.id
      FROM orders o
      WHERE NOT EXISTS (
        SELECT u.id
        FROM users u
        WHERE u.id = o.user_id
          AND u.team_id = 'team_smb'
      )
      ORDER BY o.id ASC
    `,
    expectedRows: [{ id: "ord_1" }, { id: "ord_2" }, { id: "ord_4" }],
  },
  {
    name: "correlated IN subquery",
    sql: `
      SELECT o.id
      FROM orders o
      WHERE o.user_id IN (
        SELECT u.id
        FROM users u
        WHERE u.team_id = 'team_smb'
          AND u.id = o.user_id
      )
      ORDER BY o.id ASC
    `,
    expectedRows: [{ id: "ord_3" }],
  },
  {
    name: "correlated NOT IN subquery",
    sql: `
      SELECT o.id
      FROM orders o
      WHERE o.user_id NOT IN (
        SELECT u.id
        FROM users u
        WHERE u.team_id = 'team_smb'
          AND u.id = o.user_id
      )
      ORDER BY o.id ASC
    `,
    expectedRows: [{ id: "ord_1" }, { id: "ord_2" }, { id: "ord_4" }],
  },
  {
    name: "uncorrelated NOT IN subquery",
    sql: `
      SELECT o.id
      FROM orders o
      WHERE o.user_id NOT IN (
        SELECT u.id
        FROM users u
        WHERE u.team_id = 'team_smb'
      )
      ORDER BY o.id ASC
    `,
    expectedRows: [{ id: "ord_1" }, { id: "ord_2" }, { id: "ord_4" }],
  },
  {
    name: "correlated scalar aggregate in WHERE",
    sql: `
      SELECT o.id
      FROM orders o
      WHERE o.total_cents = (
        SELECT MAX(i.total_cents)
        FROM orders i
        WHERE i.user_id = o.user_id
      )
      ORDER BY o.id ASC
    `,
    expectedRows: [{ id: "ord_2" }, { id: "ord_3" }, { id: "ord_4" }],
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
  {
    name: "correlated scalar subquery in SELECT",
    sql: `
      SELECT
        o.id,
        (
          SELECT MAX(i.total_cents)
          FROM orders i
          WHERE i.user_id = o.user_id
        ) AS user_max_total
      FROM orders o
      ORDER BY o.id ASC
    `,
    expectedRows: [
      { id: "ord_1", user_max_total: 1800 },
      { id: "ord_2", user_max_total: 1800 },
      { id: "ord_3", user_max_total: 2400 },
      { id: "ord_4", user_max_total: 9900 },
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
