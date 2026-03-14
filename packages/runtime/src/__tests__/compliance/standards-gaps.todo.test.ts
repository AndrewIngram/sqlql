import { describe, it } from "vitest";

interface StandardsGapCase {
  name: string;
  sql: string;
}

const gapCases: StandardsGapCase[] = [
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
    `,
  },
  {
    name: "non-running/advanced window frame clauses",
    sql: `
      SELECT
        SUM(total_cents) OVER (
          PARTITION BY org_id
          ORDER BY created_at
          ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS running_total
      FROM orders
    `,
  },
  {
    name: "navigation window functions LEAD/LAG",
    sql: `
      SELECT
        LEAD(total_cents) OVER (PARTITION BY org_id ORDER BY created_at) AS next_total,
        LAG(total_cents, 1, 0) OVER (PARTITION BY org_id ORDER BY created_at) AS prev_total
      FROM orders
    `,
  },
  {
    name: "navigation window function FIRST_VALUE",
    sql: `
      SELECT
        FIRST_VALUE(total_cents) OVER (PARTITION BY org_id ORDER BY created_at) AS first_total
      FROM orders
    `,
  },
  {
    name: "window + grouped aggregate in same select",
    sql: `
      SELECT
        org_id,
        COUNT(*) AS n,
        ROW_NUMBER() OVER (PARTITION BY org_id ORDER BY org_id) AS rn
      FROM orders
      GROUP BY org_id
    `,
  },
];

describe("compliance/standards-gaps", () => {
  for (const testCase of gapCases) {
    it.todo(`${testCase.name}: ${testCase.sql.replace(/\s+/g, " ").trim()}`);
  }
});
