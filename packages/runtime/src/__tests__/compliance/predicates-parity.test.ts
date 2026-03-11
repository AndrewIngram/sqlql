import { commerceRows, commerceSchema } from "@tupl/test-support/fixtures";
import { registerParityCases, type ComplianceCase } from "@tupl/test-support/runtime";

const cases: ComplianceCase[] = [
  {
    name: "single-table predicate and projection",
    sql: `
      SELECT o.id, o.total_cents
      FROM orders o
      WHERE o.org_id = 'org_1' AND o.total_cents >= 1800
      ORDER BY o.id ASC
    `,
    expectedRows: [
      { id: "ord_2", total_cents: 1800 },
      { id: "ord_3", total_cents: 2400 },
    ],
  },
  {
    name: "boolean precedence with unparenthesized OR/AND",
    sql: `
      SELECT id
      FROM orders
      WHERE org_id = 'org_1' OR status = 'paid' AND total_cents > 5000
      ORDER BY id ASC
    `,
    expectedRows: [{ id: "ord_1" }, { id: "ord_2" }, { id: "ord_3" }, { id: "ord_4" }],
  },
  {
    name: "NOT predicate",
    sql: `
      SELECT id
      FROM orders
      WHERE NOT (org_id = 'org_2')
      ORDER BY id ASC
    `,
    expectedRows: [{ id: "ord_1" }, { id: "ord_2" }, { id: "ord_3" }],
  },
  {
    name: "IN predicate with literal list",
    sql: `
      SELECT id
      FROM orders
      WHERE user_id IN ('usr_1', 'usr_3')
      ORDER BY id ASC
    `,
    expectedRows: [{ id: "ord_1" }, { id: "ord_2" }, { id: "ord_4" }],
  },
  {
    name: "BETWEEN predicate",
    sql: `
      SELECT id
      FROM orders
      WHERE total_cents BETWEEN 1500 AND 3000
      ORDER BY id ASC
    `,
    expectedRows: [{ id: "ord_2" }, { id: "ord_3" }],
  },
  {
    name: "ORDER BY multiple terms with LIMIT/OFFSET",
    sql: `
      SELECT id, created_at
      FROM orders
      ORDER BY created_at DESC, id ASC
      LIMIT 2 OFFSET 1
    `,
    expectedRows: [
      { id: "ord_3", created_at: "2026-02-04" },
      { id: "ord_2", created_at: "2026-02-03" },
    ],
  },
  {
    name: "MySQL-style LIMIT offset,count syntax",
    sql: `
      SELECT id
      FROM orders
      ORDER BY id ASC
      LIMIT 1, 2
    `,
    expectedRows: [{ id: "ord_2" }, { id: "ord_3" }],
  },
];

registerParityCases(
  "compliance/predicates-parity",
  {
    schema: commerceSchema,
    rowsByTable: commerceRows,
  },
  cases,
);
