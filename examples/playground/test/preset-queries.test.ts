import { describe, expect, it } from "vitest";

import { EXAMPLE_PACKS, serializeJson } from "../src/examples";
import { compilePlaygroundInput, createSession } from "../src/session-runtime";

describe("playground/preset-queries", () => {
  it("compiles and executes every preset query against its own pack schema/data", async () => {
    for (const pack of EXAMPLE_PACKS) {
      for (const query of pack.queries) {
        const compiled = compilePlaygroundInput(
          serializeJson(pack.schema),
          serializeJson(pack.rows),
          query.sql,
        );

        expect(
          compiled.ok,
          `[${pack.id}] ${query.label} should compile`,
        ).toBe(true);

        if (!compiled.ok) {
          continue;
        }

        const session = createSession(compiled);
        const rows = await session.runToCompletion();
        expect(
          Array.isArray(rows),
          `[${pack.id}] ${query.label} should return rows`,
        ).toBe(true);
      }
    }
  });
});
