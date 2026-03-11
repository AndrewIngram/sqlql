import { commerceRows, commerceSchema } from "@tupl/test-support/fixtures";
import { registerParityCases, type ComplianceCase } from "@tupl/test-support/runtime";

const cases: ComplianceCase[] = [
  {
    name: "INNER JOIN equality",
    sql: `
      SELECT o.id, u.email
      FROM orders o
      JOIN users u ON o.user_id = u.id
      WHERE o.org_id = 'org_1'
      ORDER BY o.id ASC
    `,
  },
  {
    name: "LEFT JOIN with post-join right predicate",
    sql: `
      SELECT o.id, u.team_id
      FROM orders o
      LEFT JOIN users u ON o.user_id = u.id
      WHERE u.team_id = 'team_enterprise'
      ORDER BY o.id ASC
    `,
  },
  {
    name: "RIGHT JOIN",
    sql: `
      SELECT o.id, u.id AS user_id
      FROM orders o
      RIGHT JOIN users u ON o.user_id = u.id
      ORDER BY user_id ASC, o.id ASC
    `,
  },
  {
    name: "FULL JOIN",
    sql: `
      SELECT o.id, u.id AS user_id
      FROM orders o
      FULL JOIN users u ON o.user_id = u.id
      ORDER BY user_id ASC, o.id ASC
    `,
  },
];

registerParityCases(
  "compliance/joins-parity",
  {
    schema: commerceSchema,
    rowsByTable: commerceRows,
  },
  cases,
);
