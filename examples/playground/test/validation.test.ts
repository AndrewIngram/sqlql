import { describe, expect, it } from "vitest";

import { EXAMPLE_PACKS, serializeJson } from "../src/examples";
import {
  PLAYGROUND_SCHEMA_JSON_SCHEMA,
  buildRowsJsonSchema,
  buildTableRowsJsonSchema,
  parseRowsText,
  parseSchemaText,
} from "../src/validation";

describe("playground/validation", () => {
  it("parses schema JSON from an example pack", () => {
    const schemaResult = parseSchemaText(serializeJson(EXAMPLE_PACKS[0]?.schema));

    expect(schemaResult.ok).toBe(true);
    expect(schemaResult.issues).toEqual([]);
  });

  it("rejects rows JSON with unknown columns", () => {
    const pack = EXAMPLE_PACKS[0];
    if (!pack) {
      throw new Error("Expected default example pack.");
    }

    const schemaResult = parseSchemaText(serializeJson(pack.schema));
    if (!schemaResult.ok || !schemaResult.schema) {
      throw new Error("Expected valid schema.");
    }

    const invalidRowsText = serializeJson({
      ...pack.rows,
      customers: [
        ...(pack.rows.customers ?? []),
        { id: "cust_bad", full_name: "Bad", region: "us-east", extra_field: 1 },
      ],
    });

    const rowsResult = parseRowsText(schemaResult.schema, invalidRowsText);
    expect(rowsResult.ok).toBe(false);
    expect(rowsResult.issues.some((issue) => issue.message.includes("Unrecognized key"))).toBe(
      true,
    );
  });

  it("builds rows JSON schema from current schema", () => {
    const pack = EXAMPLE_PACKS[0];
    if (!pack) {
      throw new Error("Expected default example pack.");
    }

    const schemaResult = parseSchemaText(serializeJson(pack.schema));
    if (!schemaResult.ok || !schemaResult.schema) {
      throw new Error("Expected valid schema.");
    }

    const rowsJsonSchema = buildRowsJsonSchema(schemaResult.schema);
    expect(rowsJsonSchema).toMatchObject({
      type: "object",
      properties: expect.objectContaining({
        customers: expect.any(Object),
      }),
    });
  });

  it("builds table rows JSON schema with enum metadata", () => {
    const pack = EXAMPLE_PACKS[0];
    if (!pack) {
      throw new Error("Expected default example pack.");
    }

    const table = pack.schema.tables.orders;
    if (!table) {
      throw new Error("Expected orders table.");
    }

    const tableSchema = buildTableRowsJsonSchema(table);
    expect(tableSchema).toMatchObject({
      type: "array",
      items: {
        type: "object",
        properties: {
          status: {
            enum: ["pending", "paid"],
          },
        },
      },
    });
  });

  it("schema editor JSON schema allows table constraints", () => {
    const tables = (PLAYGROUND_SCHEMA_JSON_SCHEMA.properties as Record<string, unknown>)
      .tables as Record<string, unknown>;
    const tableDefinition = tables.additionalProperties as Record<string, unknown>;
    const properties = tableDefinition.properties as Record<string, unknown>;
    const constraints = properties.constraints as Record<string, unknown>;
    const constraintsProperties = constraints.properties as Record<string, unknown>;

    expect(constraintsProperties).toHaveProperty("primaryKey");
    expect(constraintsProperties).toHaveProperty("unique");
    expect(constraintsProperties).toHaveProperty("foreignKeys");
    expect(constraintsProperties).toHaveProperty("checks");
  });

  it("schema editor JSON schema allows column-level foreignKey + enum metadata", () => {
    const tables = (PLAYGROUND_SCHEMA_JSON_SCHEMA.properties as Record<string, unknown>)
      .tables as Record<string, unknown>;
    const tableDefinition = tables.additionalProperties as Record<string, unknown>;
    const properties = tableDefinition.properties as Record<string, unknown>;
    const columns = properties.columns as Record<string, unknown>;
    const anyOf = (columns.additionalProperties as Record<string, unknown>).anyOf as Array<Record<
      string,
      unknown
    >>;
    const objectVariant = anyOf.find((entry) => entry.type === "object");
    if (!objectVariant) {
      throw new Error("Expected object-based column schema variant.");
    }

    const objectProperties = (objectVariant.properties as Record<string, unknown>) ?? {};
    expect(objectProperties).toHaveProperty("enum");
    expect(objectProperties).toHaveProperty("foreignKey");
    expect(objectProperties).toHaveProperty("primaryKey");
    expect(objectProperties).toHaveProperty("unique");

    const allOf = (objectVariant.allOf as Array<Record<string, unknown>> | undefined) ?? [];
    expect(allOf.length).toBeGreaterThan(0);
  });
});
