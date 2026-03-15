import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { TranslationExplainPanel } from "../src/playground-query-views";

describe("playground/translation-explain-panel", () => {
  it("renders each explain translation section", () => {
    const html = renderToStaticMarkup(
      <TranslationExplainPanel
        explain={{
          sql: "SELECT id FROM orders",
          initialRel: {
            id: "scan_1",
            kind: "scan",
            convention: "logical",
            table: "orders",
            select: ["id"],
            output: [{ name: "id" }],
          },
          rewrittenRel: {
            id: "project_1",
            kind: "project",
            convention: "logical",
            columns: [
              {
                source: { column: "id" },
                output: "id",
              },
            ],
            input: {
              id: "scan_1",
              kind: "scan",
              convention: "logical",
              table: "orders",
              select: ["id"],
              output: [{ name: "id" }],
            },
            output: [{ name: "id" }],
          },
          physicalPlan: {
            rel: {
              id: "scan_1",
              kind: "scan",
              convention: "provider:warehouse",
              table: "orders",
              select: ["id"],
              output: [{ name: "id" }],
            },
            rootStepId: "step_1",
            steps: [],
          },
          fragments: [
            {
              id: "fragment_1",
              convention: "local",
              rel: {
                id: "scan_1",
                kind: "scan",
                convention: "logical",
                table: "orders",
                select: ["id"],
                output: [{ name: "id" }],
              },
            },
          ],
          providerPlans: [
            {
              fragmentId: "fragment_2",
              provider: "warehouse",
              kind: "rel",
              rel: {
                id: "scan_2",
                kind: "scan",
                convention: "provider:warehouse",
                table: "orders",
                select: ["id"],
                output: [{ name: "id" }],
              },
              description: {
                kind: "sql",
                summary: "single statement",
                operations: [{ kind: "sql", sql: "select id from orders" }],
              },
            },
          ],
          plannerNodeCount: 1,
          diagnostics: [],
        }}
      />,
    );

    expect(html).toContain("SQL");
    expect(html).toContain("Initial Rel");
    expect(html).toContain("Rewritten Rel");
    expect(html).toContain("Physical Fragments");
    expect(html).toContain("Provider Plans");
  });
});
