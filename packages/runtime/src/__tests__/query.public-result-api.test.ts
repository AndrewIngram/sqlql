import { Result } from "better-result";
import { describe, expect, it } from "vitest";

import { createExecutableSchemaResult, type QueryRow, type SchemaDefinition } from "@tupl/schema";
import {
  createDataEntityHandle,
  type ProviderAdapter,
  type ProviderFragment,
} from "@tupl/provider-kit";
import { createSchemaBuilder, resolveTableProviderResult } from "@tupl/schema";
import { createExecutableSchemaFromProviders } from "../../../../test/support/executable-schema";
import { buildEntitySchema, buildSchema } from "../../../../test/support/schema-builder";

function createRowsProvider(rows: QueryRow[] = [{ id: "u1" }]): Omit<ProviderAdapter, "name"> {
  return {
    canExecute(fragment: ProviderFragment) {
      return fragment.kind === "scan";
    },
    async compile(fragment: ProviderFragment) {
      return Result.ok({
        provider: "warehouse",
        kind: fragment.kind,
        payload: fragment,
      });
    },
    async execute() {
      return Result.ok(rows);
    },
  };
}

describe("public result APIs", () => {
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
      _tag: "TuplPlanningError",
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
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return Result.ok({
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          });
        },
        async execute() {
          return Result.err(new Error("warehouse exploded"));
        },
      } satisfies Omit<ProviderAdapter, "name">,
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
      canExecute(fragment: ProviderFragment) {
        return fragment.kind === "scan";
      },
      async compile(fragment: ProviderFragment) {
        return Result.ok({
          provider: "actual",
          kind: fragment.kind,
          payload: fragment,
        });
      },
      async execute() {
        return Result.ok([{ id: "u1" }]);
      },
    } satisfies ProviderAdapter;

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

  it("returns tagged setup errors from createSessionResult", () => {
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

    const result = executableSchema.createSessionResult({
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
