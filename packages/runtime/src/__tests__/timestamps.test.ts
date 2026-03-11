import { describe, expect, it } from "vitest";

import { withQueryHarness } from "@tupl/test-support/runtime";
import { buildEntitySchema } from "@tupl/test-support/schema";

const EMPTY_CONTEXT = {} as const;

describe("query/timestamps", () => {
  it("normalizes Date values to ISO strings for filtering and ordering", async () => {
    const schema = buildEntitySchema({
      events: {
        columns: {
          id: { type: "text", nullable: false },
          created_at: { type: "timestamp", nullable: false },
        },
      },
    });

    await withQueryHarness(
      {
        schema,
        rowsByTable: {
          events: [
            { id: "evt_1", created_at: new Date("2026-02-01T10:00:00.000Z") },
            { id: "evt_2", created_at: new Date("2026-02-03T10:00:00.000Z") },
            { id: "evt_3", created_at: new Date("2026-02-05T10:00:00.000Z") },
          ],
        },
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT id
            FROM events
            WHERE created_at >= '2026-02-03T00:00:00.000Z'
            ORDER BY created_at ASC
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
        expect(actual).toEqual([{ id: "evt_2" }, { id: "evt_3" }]);
      },
    );
  });

  it("supports MIN/MAX aggregation on normalized timestamp values", async () => {
    const schema = buildEntitySchema({
      events: {
        columns: {
          id: { type: "text", nullable: false },
          created_at: { type: "timestamp", nullable: false },
        },
      },
    });

    await withQueryHarness(
      {
        schema,
        rowsByTable: {
          events: [
            { id: "evt_1", created_at: new Date("2026-02-01T10:00:00.000Z") },
            { id: "evt_2", created_at: new Date("2026-02-03T10:00:00.000Z") },
            { id: "evt_3", created_at: new Date("2026-02-05T10:00:00.000Z") },
          ],
        },
      },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(
          `
            SELECT
              MIN(created_at) AS min_created_at,
              MAX(created_at) AS max_created_at
            FROM events
          `,
          EMPTY_CONTEXT,
        );

        expect(actual).toEqual(expected);
      },
    );
  });
});
