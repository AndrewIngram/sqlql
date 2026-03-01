import { z } from "zod";
import {
  defineSchema,
  type QueryRow,
  type SchemaDefinition,
  type TableColumnDefinition,
} from "sqlql";

import {
  isColumnNullable,
  readColumnType,
  type RowsParseResult,
  type SchemaParseResult,
  type SchemaValidationIssue,
} from "./types";

const sqlScalarTypeSchema = z.enum(["text", "integer", "boolean", "timestamp"]);

const queryDefaultsSchema = z
  .object({
    filterable: z.union([z.literal("all"), z.array(z.string())]).optional(),
    sortable: z.union([z.literal("all"), z.array(z.string())]).optional(),
    maxRows: z.number().int().nonnegative().nullable().optional(),
  })
  .strict();

const primaryKeySchema = z
  .object({
    columns: z.array(z.string()).min(1),
    name: z.string().optional(),
  })
  .strict();

const uniqueSchema = z
  .object({
    columns: z.array(z.string()).min(1),
    name: z.string().optional(),
  })
  .strict();

const referentialActionSchema = z.enum([
  "NO ACTION",
  "RESTRICT",
  "CASCADE",
  "SET NULL",
  "SET DEFAULT",
]);

const foreignKeySchema = z
  .object({
    columns: z.array(z.string()).min(1),
    references: z
      .object({
        table: z.string().min(1),
        columns: z.array(z.string()).min(1),
      })
      .strict(),
    name: z.string().optional(),
    onDelete: referentialActionSchema.optional(),
    onUpdate: referentialActionSchema.optional(),
  })
  .strict();

const columnDefinitionSchema = z.union([
  sqlScalarTypeSchema,
  z
    .object({
      type: sqlScalarTypeSchema,
      nullable: z.boolean().optional(),
    })
    .strict(),
]);

const tableSchema = z
  .object({
    columns: z.record(z.string().min(1), columnDefinitionSchema),
    query: queryDefaultsSchema.optional(),
    constraints: z
      .object({
        primaryKey: primaryKeySchema.optional(),
        unique: z.array(uniqueSchema).optional(),
        foreignKeys: z.array(foreignKeySchema).optional(),
      })
      .strict()
      .optional(),
  })
  .strict();

const schemaSchema = z
  .object({
    defaults: z
      .object({
        query: queryDefaultsSchema.partial().optional(),
      })
      .strict()
      .optional(),
    tables: z.record(z.string().min(1), tableSchema),
  })
  .strict();

function issuePath(path: Array<string | number>): string {
  if (path.length === 0) {
    return "$";
  }

  return path
    .map((segment, index) => {
      if (typeof segment === "number") {
        return `[${segment}]`;
      }
      return index === 0 ? segment : `.${segment}`;
    })
    .join("");
}

function zodIssues(error: z.ZodError): SchemaValidationIssue[] {
  return error.issues.map((issue) => ({
    path: issuePath(issue.path),
    message: issue.message,
  }));
}

export function parseSchemaText(value: string): SchemaParseResult {
  let parsedJson: unknown;
  try {
    parsedJson = JSON.parse(value);
  } catch (error) {
    return {
      ok: false,
      issues: [
        {
          path: "$",
          message: error instanceof Error ? error.message : "Invalid JSON.",
        },
      ],
    };
  }

  const parsedSchema = schemaSchema.safeParse(parsedJson);
  if (!parsedSchema.success) {
    return {
      ok: false,
      issues: zodIssues(parsedSchema.error),
    };
  }

  try {
    const schema = defineSchema(parsedSchema.data as SchemaDefinition);
    return {
      ok: true,
      schema,
      issues: [],
    };
  } catch (error) {
    return {
      ok: false,
      issues: [
        {
          path: "$",
          message: error instanceof Error ? error.message : "Invalid schema.",
        },
      ],
    };
  }
}

function validatorForColumn(column: TableColumnDefinition): z.ZodType<unknown> {
  const type = readColumnType(column);
  let validator: z.ZodType<unknown>;

  switch (type) {
    case "text":
    case "timestamp":
      validator = z.string();
      break;
    case "integer":
      validator = z.number().finite();
      break;
    case "boolean":
      validator = z.boolean();
      break;
  }

  if (isColumnNullable(column)) {
    return validator.nullable();
  }

  return validator;
}

export function parseRowsText(schema: SchemaDefinition, value: string): RowsParseResult {
  let parsedJson: unknown;
  try {
    parsedJson = JSON.parse(value);
  } catch (error) {
    return {
      ok: false,
      issues: [
        {
          path: "$",
          message: error instanceof Error ? error.message : "Invalid JSON.",
        },
      ],
    };
  }

  const tableEntries = Object.entries(schema.tables);
  const tableSchemas = tableEntries.map(([tableName, tableDefinition]) => {
    const rowShape: Record<string, z.ZodType<unknown>> = {};
    for (const [columnName, columnDefinition] of Object.entries(tableDefinition.columns)) {
      rowShape[columnName] = validatorForColumn(columnDefinition);
    }

    return [tableName, z.array(z.object(rowShape).strict()).optional()] as const;
  });

  const recordShape = Object.fromEntries(tableSchemas);
  const rowsSchema = z.object(recordShape).strict();
  const parsedRows = rowsSchema.safeParse(parsedJson);

  if (!parsedRows.success) {
    return {
      ok: false,
      issues: zodIssues(parsedRows.error),
    };
  }

  const normalizedRows = Object.fromEntries(
    tableEntries.map(([tableName]) => [tableName, parsedRows.data[tableName] ?? []]),
  ) as Record<string, QueryRow[]>;

  return {
    ok: true,
    rows: normalizedRows,
    issues: [],
  };
}

function toJsonSchemaType(column: TableColumnDefinition): { type: string | string[] } {
  const type = readColumnType(column);
  const baseType = type === "integer" ? "number" : type === "boolean" ? "boolean" : "string";

  if (isColumnNullable(column)) {
    return {
      type: [baseType, "null"],
    };
  }

  return {
    type: baseType,
  };
}

export function buildRowsJsonSchema(schema: SchemaDefinition): Record<string, unknown> {
  const tableProperties: Record<string, unknown> = {};

  for (const [tableName, table] of Object.entries(schema.tables)) {
    const columnProperties: Record<string, unknown> = {};
    const required: string[] = [];

    for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
      columnProperties[columnName] = toJsonSchemaType(columnDefinition);
      required.push(columnName);
    }

    tableProperties[tableName] = {
      type: "array",
      items: {
        type: "object",
        additionalProperties: false,
        required,
        properties: columnProperties,
      },
    };
  }

  return {
    type: "object",
    additionalProperties: false,
    properties: tableProperties,
  };
}

export const PLAYGROUND_SCHEMA_JSON_SCHEMA: Record<string, unknown> = {
  type: "object",
  additionalProperties: false,
  required: ["tables"],
  properties: {
    defaults: {
      type: "object",
      additionalProperties: false,
      properties: {
        query: {
          type: "object",
          additionalProperties: false,
          properties: {
            filterable: {
              anyOf: [
                { type: "string", enum: ["all"] },
                {
                  type: "array",
                  items: { type: "string" },
                },
              ],
            },
            sortable: {
              anyOf: [
                { type: "string", enum: ["all"] },
                {
                  type: "array",
                  items: { type: "string" },
                },
              ],
            },
            maxRows: {
              anyOf: [{ type: "integer", minimum: 0 }, { type: "null" }],
            },
          },
        },
      },
    },
    tables: {
      type: "object",
      additionalProperties: {
        type: "object",
        additionalProperties: false,
        required: ["columns"],
        properties: {
          columns: {
            type: "object",
            additionalProperties: {
              anyOf: [
                {
                  type: "string",
                  enum: ["text", "integer", "boolean", "timestamp"],
                },
                {
                  type: "object",
                  additionalProperties: false,
                  required: ["type"],
                  properties: {
                    type: {
                      type: "string",
                      enum: ["text", "integer", "boolean", "timestamp"],
                    },
                    nullable: { type: "boolean" },
                  },
                },
              ],
            },
          },
          query: {
            type: "object",
            additionalProperties: false,
            properties: {
              filterable: {
                anyOf: [
                  { type: "string", enum: ["all"] },
                  { type: "array", items: { type: "string" } },
                ],
              },
              sortable: {
                anyOf: [
                  { type: "string", enum: ["all"] },
                  { type: "array", items: { type: "string" } },
                ],
              },
              maxRows: {
                anyOf: [{ type: "integer", minimum: 0 }, { type: "null" }],
              },
            },
          },
          constraints: {
            type: "object",
            additionalProperties: false,
            properties: {
              primaryKey: {
                type: "object",
                additionalProperties: false,
                required: ["columns"],
                properties: {
                  columns: {
                    type: "array",
                    minItems: 1,
                    items: { type: "string" },
                  },
                  name: { type: "string" },
                },
              },
              unique: {
                type: "array",
                items: {
                  type: "object",
                  additionalProperties: false,
                  required: ["columns"],
                  properties: {
                    columns: {
                      type: "array",
                      minItems: 1,
                      items: { type: "string" },
                    },
                    name: { type: "string" },
                  },
                },
              },
              foreignKeys: {
                type: "array",
                items: {
                  type: "object",
                  additionalProperties: false,
                  required: ["columns", "references"],
                  properties: {
                    columns: {
                      type: "array",
                      minItems: 1,
                      items: { type: "string" },
                    },
                    references: {
                      type: "object",
                      additionalProperties: false,
                      required: ["table", "columns"],
                      properties: {
                        table: { type: "string" },
                        columns: {
                          type: "array",
                          minItems: 1,
                          items: { type: "string" },
                        },
                      },
                    },
                    name: { type: "string" },
                    onDelete: {
                      type: "string",
                      enum: ["NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT"],
                    },
                    onUpdate: {
                      type: "string",
                      enum: ["NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT"],
                    },
                  },
                },
              },
            },
          },
        },
      },
    },
  },
};
