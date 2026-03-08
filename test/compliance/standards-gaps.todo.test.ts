import { describe, it } from "vitest";

interface StandardsGapCase {
  name: string;
  sql: string;
}

const gapCases: StandardsGapCase[] = [
  {
    name: "correlated subquery in WHERE",
    sql: `
      SELECT o.id
      FROM orders o
      WHERE EXISTS (
        SELECT 1
        FROM users u
        WHERE u.id = o.user_id
      )
    `,
  },
  {
    name: "subquery in FROM",
    sql: `
      SELECT s.id
      FROM (SELECT id FROM orders) s
    `,
  },
  {
    name: "recursive CTE",
    sql: `
      WITH RECURSIVE x(n) AS (
        SELECT 1
        UNION ALL
        SELECT n + 1 FROM x WHERE n < 3
      )
      SELECT n FROM x
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
    name: "named WINDOW clause reference",
    sql: `
      SELECT
        SUM(total_cents) OVER w AS running_total
      FROM orders
      WINDOW w AS (
        PARTITION BY org_id
        ORDER BY created_at
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )
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
