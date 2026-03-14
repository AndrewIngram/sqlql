import { describe, it } from "vitest";

interface StandardsGapCase {
  name: string;
  sql: string;
}

const gapCases: StandardsGapCase[] = [];

describe("compliance/standards-gaps", () => {
  for (const testCase of gapCases) {
    it.todo(`${testCase.name}: ${testCase.sql.replace(/\s+/g, " ").trim()}`);
  }
});
