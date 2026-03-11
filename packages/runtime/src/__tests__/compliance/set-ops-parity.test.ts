import { commerceRows, commerceSchema } from "@tupl/test-support/fixtures";
import { registerParityCases, type ComplianceCase } from "@tupl/test-support/runtime";

const cases: ComplianceCase[] = [
  {
    name: "SELECT DISTINCT with ORDER/LIMIT/OFFSET",
    sql: `
      SELECT DISTINCT org_id
      FROM orders
      ORDER BY org_id DESC
      LIMIT 1 OFFSET 0
    `,
    expectedRows: [{ org_id: "org_2" }],
  },
  {
    name: "UNION ALL",
    sql: `
      SELECT user_id AS id FROM orders WHERE org_id = 'org_1'
      UNION ALL
      SELECT id FROM users
    `,
  },
  {
    name: "UNION",
    sql: `
      SELECT user_id AS id FROM orders
      UNION
      SELECT id FROM users
    `,
  },
  {
    name: "INTERSECT",
    sql: `
      SELECT user_id AS id FROM orders
      INTERSECT
      SELECT id FROM users
    `,
  },
  {
    name: "EXCEPT",
    sql: `
      SELECT id FROM users
      EXCEPT
      SELECT user_id AS id FROM orders WHERE org_id = 'org_1'
    `,
    expectedRows: [{ id: "usr_3" }],
  },
];

registerParityCases(
  "compliance/set-ops-parity",
  {
    schema: commerceSchema,
    rowsByTable: commerceRows,
  },
  cases,
);
