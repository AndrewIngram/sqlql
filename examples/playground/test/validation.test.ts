import { describe, expect, it } from "vitest";

import {
  DB_PROVIDER_MODULE_ID,
  DEFAULT_DB_PROVIDER_CODE,
  DEFAULT_FACADE_SCHEMA_CODE,
  DEFAULT_GENERATED_DB_FILE_CODE,
  DEFAULT_KV_PROVIDER_CODE,
  FACADE_SCHEMA,
  GENERATED_DB_MODULE_ID,
  KV_PROVIDER_MODULE_ID,
  SCENARIO_PRESETS,
  serializeJson,
} from "../src/examples";
import {
  buildRowsJsonSchema,
  buildTableRowsJsonSchema,
  parseDownstreamRowsText,
  parseFacadeSchemaCode,
  parseFacadeSchemaText,
} from "../src/validation";

describe("playground/validation", () => {
  it("parses facade schema TypeScript module", async () => {
    const schemaResult = await parseFacadeSchemaCode(DEFAULT_FACADE_SCHEMA_CODE, {
      modules: {
        [DB_PROVIDER_MODULE_ID]: DEFAULT_DB_PROVIDER_CODE,
        [GENERATED_DB_MODULE_ID]: DEFAULT_GENERATED_DB_FILE_CODE,
        [KV_PROVIDER_MODULE_ID]: DEFAULT_KV_PROVIDER_CODE,
      },
    });

    expect(schemaResult.ok).toBe(true);
    expect(schemaResult.issues).toEqual([]);
  });

  it("rejects schema modules missing exported executableSchema", async () => {
    const schemaResult = await parseFacadeSchemaCode("export const notSchema = 1;");

    expect(schemaResult.ok).toBe(false);
    expect(schemaResult.issues[0]?.message).toContain("SCHEMA_EXPORT_MISSING");
    expect(schemaResult.issues[0]?.message).toContain("executableSchema");
  });

  it("rejects schema modules with invalid executable schema export", async () => {
    const schemaResult = await parseFacadeSchemaCode("export const executableSchema = { nope: true };");

    expect(schemaResult.ok).toBe(false);
    expect(schemaResult.issues[0]?.message).toContain("SCHEMA_EXPORT_INVALID");
  });

  it("rejects schema modules that throw at runtime", async () => {
    const schemaResult = await parseFacadeSchemaCode(
      'const fail = (): never => { throw new Error("boom"); }; fail(); export const executableSchema = {};',
    );

    expect(schemaResult.ok).toBe(false);
    expect(schemaResult.issues[0]?.message).toContain("SCHEMA_EXEC_ERROR");
  });

  it("rejects downstream rows JSON with unknown columns", () => {
    const scenario = SCENARIO_PRESETS[0];
    if (!scenario) {
      throw new Error("Expected default scenario.");
    }

    const schemaResult = parseFacadeSchemaText(serializeJson(FACADE_SCHEMA));
    if (!schemaResult.ok || !schemaResult.schema) {
      throw new Error("Expected valid schema.");
    }

    const invalidRowsText = serializeJson({
      ...scenario.rows,
      users: [
        ...(scenario.rows.users ?? []),
        {
          id: "u_bad",
          org_id: "org_acme",
          email: "bad@example.com",
          display_name: "Bad",
          role: "buyer",
          extra_field: 1,
        },
      ],
    });

    const rowsResult = parseDownstreamRowsText(invalidRowsText);
    expect(rowsResult.ok).toBe(false);
    expect(rowsResult.issues.some((issue) => issue.message.includes("Unrecognized key"))).toBe(
      true,
    );
  });

  it("builds rows JSON schema from current schema", () => {
    const schemaResult = parseFacadeSchemaText(serializeJson(FACADE_SCHEMA));
    if (!schemaResult.ok || !schemaResult.schema) {
      throw new Error("Expected valid schema.");
    }

    const rowsJsonSchema = buildRowsJsonSchema(schemaResult.schema);
    expect(rowsJsonSchema).toMatchObject({
      type: "object",
      properties: expect.objectContaining({
        my_orders: expect.any(Object),
      }),
    });
  });

  it("builds table rows JSON schema with enum metadata", () => {
    const table = FACADE_SCHEMA.tables.my_orders;
    if (!table) {
      throw new Error("Expected my_orders table.");
    }

    const tableSchema = buildTableRowsJsonSchema(table);
    expect(tableSchema).toMatchObject({
      type: "array",
      items: {
        type: "object",
        properties: {
          status: {
            enum: ["pending", "paid", "shipped"],
          },
        },
      },
    });
  });
});
