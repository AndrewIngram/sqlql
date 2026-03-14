import { Result } from "better-result";
import { describe, expect, it } from "vitest";

import { createExecutableSchemaFromProviders } from "@tupl/test-support/runtime";
import { buildEntitySchema } from "@tupl/test-support/schema";

describe("runtime/explain", () => {
  it("returns staged explain artifacts for translation introspection", async () => {
    const schema = buildEntitySchema({
      orders: {
        provider: "warehouse",
        columns: {
          id: "text",
          user_id: "text",
        },
      },
      users: {
        provider: "warehouse",
        columns: {
          id: "text",
          email: "text",
        },
      },
    });
    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute() {
          return true;
        },
        async compile(fragment) {
          return Result.ok({
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        describeCompiledPlan(plan: { kind: string }) {
          return {
            kind: "test_plan",
            summary: `compiled ${plan.kind}`,
            operations: [{ kind: "rel", target: "warehouse" }],
          };
        },
        async execute() {
          return Result.ok([]);
        },
      },
    });

    const explained = await executableSchema.explain({
      context: {},
      sql: `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
      `,
    });

    expect(explained).toMatchObject({
      sql: "SELECT o.id, u.email FROM orders o JOIN users u ON o.user_id = u.id",
      plannerNodeCount: expect.any(Number),
      initialRel: expect.any(Object),
      rewrittenRel: expect.any(Object),
      physicalPlan: expect.any(Object),
      fragments: expect.arrayContaining([
        expect.objectContaining({
          convention: expect.any(String),
          rel: expect.any(Object),
        }),
      ]),
      providerPlans: expect.arrayContaining([
        expect.objectContaining({
          provider: "warehouse",
          description: expect.objectContaining({
            kind: "test_plan",
            operations: expect.any(Array),
          }),
        }),
      ]),
    });
  });

  it("marks provider descriptions as unavailable when the adapter omits a describe hook", async () => {
    const schema = buildEntitySchema({
      orders: {
        provider: "warehouse",
        columns: {
          id: "text",
        },
      },
    });
    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute() {
          return true;
        },
        async compile(fragment) {
          return Result.ok({
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute() {
          return Result.ok([]);
        },
      },
    });

    const explained = await executableSchema.explain({
      context: {},
      sql: "SELECT id FROM orders",
    });

    expect(explained.providerPlans).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          provider: "warehouse",
          descriptionUnavailable: true,
        }),
      ]),
    );
  });
});
