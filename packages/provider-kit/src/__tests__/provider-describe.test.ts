import { Result } from "better-result";
import { describe, expect, it } from "vitest";

import { createRelationalProviderAdapter } from "@tupl/provider-kit";

describe("provider/describe-compiled-plan", () => {
  it("surfaces describeCompiledPlan hooks on relational adapters", async () => {
    const adapter = createRelationalProviderAdapter({
      name: "warehouse",
      entities: {
        orders: {},
      },
      resolveRelCompileStrategy() {
        return "basic";
      },
      describeCompiledPlan({ plan }) {
        return {
          kind: "relational",
          summary: `compiled ${plan.kind}`,
          operations: [{ kind: "scan", target: "orders" }],
        };
      },
      async executeCompiledPlan() {
        return Result.ok([]);
      },
    });

    const compileResult = await adapter.compile(
      {
        kind: "rel",
        provider: "warehouse",
        rel: {
          id: "scan_orders",
          kind: "scan",
          convention: "provider:warehouse",
          table: "orders",
          select: ["id"],
          output: [{ name: "id" }],
        },
      },
      {},
    );
    if (Result.isError(compileResult)) {
      throw compileResult.error;
    }

    await expect(adapter.describeCompiledPlan?.(compileResult.value, {})).resolves.toEqual({
      kind: "relational",
      summary: "compiled rel",
      operations: [{ kind: "scan", target: "orders" }],
    });
  });
});
