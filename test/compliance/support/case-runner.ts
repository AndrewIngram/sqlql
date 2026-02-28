import { describe, expect, it } from "vitest";

import type { QueryRow, SchemaDefinition } from "../../../src";
import { withQueryHarness, type RowsByTable } from "../../support/query-harness";

export interface ComplianceCase {
  name: string;
  sql: string;
  expectedRows?: QueryRow[];
}

const EMPTY_CONTEXT = {} as const;

export function registerParityCases<TSchema extends SchemaDefinition>(
  title: string,
  options: {
    schema: TSchema;
    rowsByTable: RowsByTable<TSchema>;
  },
  cases: ComplianceCase[],
): void {
  describe(title, () => {
    for (const testCase of cases) {
      it(testCase.name, async () => {
        const { actual, expected } = await withQueryHarness(options, (harness) =>
          harness.runAgainstBoth(testCase.sql, EMPTY_CONTEXT),
        );

        expect(actual).toEqual(expected);
        if (testCase.expectedRows) {
          expect(actual).toEqual(testCase.expectedRows);
        }
      });
    }
  });
}
