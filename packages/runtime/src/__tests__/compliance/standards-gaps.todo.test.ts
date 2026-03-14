import { describe, it } from "vitest";

interface StandardsGapCase {
  name: string;
  sql: string;
}

const gapCases: StandardsGapCase[] = [
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
