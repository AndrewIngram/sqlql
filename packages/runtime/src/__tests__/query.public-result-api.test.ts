import { Result } from "better-result";
import { describe, expect, it } from "vitest";
import type { RelNode } from "@tupl/foundation";

import { createExecutableSchemaResult } from "@tupl/runtime";
import { createExecutableSchemaSessionResult } from "@tupl/runtime/session";
import type { QueryRow, SchemaDefinition } from "@tupl/schema-model";
import { createDataEntityHandle, type FragmentProviderAdapter } from "@tupl/provider-kit";
import type { LookupManyCapableProviderAdapter } from "@tupl/provider-kit/shapes";
import { createSchemaBuilder, resolveTableProviderResult } from "@tupl/schema-model";
import { createExecutableSchemaFromProviders } from "@tupl/test-support/runtime";
import { buildEntitySchema, buildSchema } from "@tupl/test-support/schema";

type TestProvider = Omit<FragmentProviderAdapter, "name"> &
  Partial<LookupManyCapableProviderAdapter>;

function createRowsProvider(rows: QueryRow[] = [{ id: "u1" }]): TestProvider {
  return {
    canExecute(rel: RelNode) {
      return rel.kind === "scan";
    },
    async compile(rel: RelNode) {
      return Result.ok({
        provider: "warehouse",
        kind: "rel",
        payload: rel,
      });
    },
    async execute() {
      return Result.ok(rows);
    },
  };
}

describe("public result APIs", () => {
  it("executes provider-owned root rel queries through the canonical executor path", async () => {
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

    let sawRelCompile = false;

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute() {
          return true;
        },
        async compile(rel: RelNode) {
          sawRelCompile = true;

          return Result.ok({
            provider: "warehouse",
            kind: "rel",
            payload: rel,
          });
        },
        async execute() {
          return Result.ok([{ id: "o1", email: "a@example.com" }]);
        },
      } satisfies TestProvider,
    });

    const result = await executableSchema.queryResult({
      context: {},
      sql: `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
      `,
    });

    expect(Result.isOk(result)).toBe(true);
    if (Result.isError(result)) {
      throw new Error("Expected queryResult to succeed.");
    }

    expect(result.value).toEqual([{ id: "o1", email: "a@example.com" }]);
    expect(sawRelCompile).toBe(true);
  });

  it("returns tagged parse errors from queryResult", async () => {
    const schema = buildEntitySchema({
      users: {
        provider: "warehouse",
        columns: {
          id: "text",
        },
      },
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: createRowsProvider(),
    });

    const result = await executableSchema.queryResult({
      context: {},
      sql: "INSERT INTO users VALUES ('u1')",
    });

    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected queryResult to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplParseError",
      message: "Only SELECT statements are currently supported.",
    });
  });

  it("returns tagged planning errors from queryResult", async () => {
    const schema = buildEntitySchema({
      users: {
        provider: "warehouse",
        columns: {
          id: "text",
        },
      },
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: createRowsProvider(),
    });

    const result = await executableSchema.queryResult({
      context: {},
      sql: "SELECT id FROM missing_table",
    });

    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected queryResult to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "RelLoweringError",
      message: "Unknown table: missing_table",
    });
  });

  it("returns tagged runtime errors from queryResult", async () => {
    const schema = buildEntitySchema({
      users: {
        provider: "warehouse",
        columns: {
          id: "text",
        },
      },
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: {
        canExecute(rel: RelNode) {
          return rel.kind === "scan";
        },
        async compile(rel: RelNode) {
          return Result.ok({
            provider: "warehouse",
            kind: "rel",
            payload: rel,
          });
        },
        async execute() {
          return Result.err(new Error("warehouse exploded"));
        },
      } satisfies TestProvider,
    });

    const result = await executableSchema.queryResult({
      context: {},
      sql: "SELECT id FROM users",
    });

    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected queryResult to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplExecutionError",
      message: "warehouse exploded",
    });
  });

  it("returns tagged runtime errors when executable schema provider bindings are inconsistent", () => {
    const adapter = {
      name: "actual",
      canExecute(rel: RelNode) {
        return rel.kind === "scan";
      },
      async compile(rel: RelNode) {
        return Result.ok({
          provider: "actual",
          kind: "rel",
          payload: rel,
        });
      },
      async execute() {
        return Result.ok([{ id: "u1" }]);
      },
    } satisfies FragmentProviderAdapter;

    const builder = createSchemaBuilder<Record<string, never>>();
    builder.table(
      "users",
      createDataEntityHandle({
        entity: "users",
        provider: "warehouse",
        adapter,
      }),
      {
        columns: {
          id: "text",
        },
      },
    );

    const result = createExecutableSchemaResult(builder);

    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected createExecutableSchemaResult to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplRuntimeError",
      message:
        "Table users is bound to provider warehouse, but the attached adapter is named actual.",
    });
  });

  it("returns tagged setup errors from createExecutableSchemaSessionResult", () => {
    const schema = buildEntitySchema({
      users: {
        provider: "warehouse",
        columns: {
          id: "text",
        },
      },
    });

    const executableSchema = createExecutableSchemaFromProviders(schema, {
      warehouse: createRowsProvider(),
    });

    const result = createExecutableSchemaSessionResult(executableSchema, {
      context: {},
      sql: "INSERT INTO users VALUES ('u1')",
    });

    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected createSessionResult to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplParseError",
      message: "Only SELECT statements are currently supported.",
    });
  });

  it("returns tagged errors from createExecutableSchemaResult", () => {
    const builder = createSchemaBuilder<Record<string, never>>();
    builder.table(
      "users",
      createDataEntityHandle({
        entity: "users",
        provider: "warehouse",
      }),
      {
        columns: {
          id: "text",
        },
      },
    );

    const result = createExecutableSchemaResult(builder);
    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected createExecutableSchemaResult to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplRuntimeError",
      message:
        "Table users must be declared from a provider-owned entity via table(name, provider.entities.someTable, config).",
    });
  });
});

describe("resolveTableProviderResult", () => {
  it("returns a tagged error for views without a direct provider", () => {
    const schema = buildSchema((builder) => {
      builder.view("active_users", ({ scan }) => scan("users"), {
        columns: {
          id: { source: "users.id" },
        },
      });
    });

    const result = resolveTableProviderResult(schema, "active_users");
    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected resolveTableProviderResult to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplProviderBindingError",
      message: "View table active_users does not have a direct provider binding.",
    });
  });

  it("returns a tagged error for unknown tables", () => {
    const schema: SchemaDefinition = {
      tables: {},
    };

    const result = resolveTableProviderResult(schema, "missing");
    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected resolveTableProviderResult to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplProviderBindingError",
      message: "Unknown table: missing",
    });
  });

  it("returns a tagged error when a table is missing a provider mapping", () => {
    const schema: SchemaDefinition = {
      tables: {
        users: {
          columns: {
            id: "text",
          },
        },
      },
    };

    const result = resolveTableProviderResult(schema, "users");
    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected resolveTableProviderResult to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplProviderBindingError",
      message: "Table users is missing required provider mapping.",
    });
  });
});
