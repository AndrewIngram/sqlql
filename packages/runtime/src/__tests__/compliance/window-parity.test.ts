import { commerceRows, commerceSchema } from "@tupl/test-support/fixtures";
import { registerParityCases, type ComplianceCase } from "@tupl/test-support/runtime";

const cases: ComplianceCase[] = [
  {
    name: "ROW_NUMBER over partition/order",
    sql: `
      SELECT
        o.id,
        o.org_id,
        ROW_NUMBER() OVER (PARTITION BY o.org_id ORDER BY o.created_at ASC) AS rn
      FROM orders o
      ORDER BY o.id ASC
    `,
  },
  {
    name: "RANK and DENSE_RANK over ordered values",
    sql: `
      SELECT
        o.id,
        o.org_id,
        RANK() OVER (PARTITION BY o.org_id ORDER BY o.total_cents DESC) AS rnk,
        DENSE_RANK() OVER (PARTITION BY o.org_id ORDER BY o.total_cents DESC) AS dense_rnk
      FROM orders o
      ORDER BY o.id ASC
    `,
  },
  {
    name: "COUNT(*) window partition",
    sql: `
      SELECT
        o.id,
        COUNT(*) OVER (PARTITION BY o.org_id) AS org_count
      FROM orders o
      ORDER BY o.id ASC
    `,
  },
  {
    name: "running SUM window partition/order",
    sql: `
      SELECT
        o.id,
        SUM(o.total_cents) OVER (PARTITION BY o.org_id ORDER BY o.created_at ASC) AS running_total
      FROM orders o
      ORDER BY o.id ASC
    `,
  },
  {
    name: "bounded ROWS frame",
    sql: `
      SELECT
        o.id,
        SUM(o.total_cents) OVER (
          PARTITION BY o.org_id
          ORDER BY o.created_at ASC
          ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS bounded_total
      FROM orders o
      ORDER BY o.id ASC
    `,
  },
  {
    name: "grouped aggregate with window projection",
    sql: `
      SELECT
        o.org_id,
        COUNT(*) AS org_count,
        ROW_NUMBER() OVER (PARTITION BY org_id ORDER BY org_id) AS rn
      FROM orders o
      GROUP BY o.org_id
      ORDER BY o.org_id ASC
    `,
  },
  {
    name: "navigation functions LEAD/LAG/FIRST_VALUE",
    sql: `
      SELECT
        o.id,
        LEAD(o.total_cents) OVER (PARTITION BY o.org_id ORDER BY o.created_at ASC) AS next_total,
        LAG(o.total_cents, 1, 0) OVER (PARTITION BY o.org_id ORDER BY o.created_at ASC) AS prev_total,
        FIRST_VALUE(o.total_cents) OVER (PARTITION BY o.org_id ORDER BY o.created_at ASC) AS first_total
      FROM orders o
      ORDER BY o.id ASC
    `,
  },
];

registerParityCases(
  "compliance/window-parity",
  {
    schema: commerceSchema,
    rowsByTable: commerceRows,
  },
  cases,
);
