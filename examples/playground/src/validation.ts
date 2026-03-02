import { z } from "zod";
import {
  defineSchema,
  type QueryRow,
  type SchemaDefinition,
  type TableColumnDefinition,
  type TableDefinition,
} from "sqlql";

import {
  isColumnNullable,
  readColumnType,
  type RowsParseResult,
  type SchemaParseResult,
  type SchemaValidationIssue,
} from "./types";

const sqlScalarTypeSchema = z.enum(["text", "integer", "boolean", "timestamp"]);

const queryRejectSchema = z
  .object({
    requiresLimit: z.boolean().optional(),
    forbidFullScan: z.boolean().optional(),
    requireAnyFilterOn: z.array(z.string()).optional(),
  })
  .strict();

const queryFallbackSchema = z
  .object({
    filters: z.enum(["allow_local", "require_pushdown"]).optional(),
    sorting: z.enum(["allow_local", "require_pushdown"]).optional(),
    aggregates: z.enum(["allow_local", "require_pushdown"]).optional(),
    limitOffset: z.enum(["allow_local", "require_pushdown"]).optional(),
  })
  .strict();

const queryDefaultsSchema = z
  .object({
    maxRows: z.number().int().nonnegative().nullable().optional(),
    reject: queryRejectSchema.optional(),
    fallback: queryFallbackSchema.optional(),
    filterable: z.union([z.literal("all"), z.array(z.string())]).optional(),
    sortable: z.union([z.literal("all"), z.array(z.string())]).optional(),
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

const columnForeignKeySchema = z
  .object({
    table: z.string().min(1),
    column: z.string().min(1),
    name: z.string().optional(),
    onDelete: referentialActionSchema.optional(),
    onUpdate: referentialActionSchema.optional(),
  })
  .strict();

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

const checkConstraintSchema = z
  .object({
    kind: z.literal("in"),
    column: z.string().min(1),
    values: z.array(z.union([z.string(), z.number(), z.boolean(), z.null()])).min(1),
    name: z.string().optional(),
  })
  .strict();

const columnObjectDefinitionSchema = z
  .object({
    type: sqlScalarTypeSchema,
    nullable: z.boolean().optional(),
    filterable: z.boolean().optional(),
    sortable: z.boolean().optional(),
    primaryKey: z.boolean().optional(),
    unique: z.boolean().optional(),
    enum: z.array(z.string()).min(1).optional(),
    foreignKey: columnForeignKeySchema.optional(),
    description: z.string().optional(),
  })
  .strict()
  .superRefine((value, ctx) => {
    if (value.primaryKey === true && value.unique === true) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["unique"],
        message: "A column cannot be both primaryKey and unique.",
      });
    }
  });

const columnDefinitionSchema = z.union([sqlScalarTypeSchema, columnObjectDefinitionSchema]);

const tableSchema = z
  .object({
    columns: z.record(z.string().min(1), columnDefinitionSchema),
    query: queryDefaultsSchema.optional(),
    constraints: z
      .object({
        primaryKey: primaryKeySchema.optional(),
        unique: z.array(uniqueSchema).optional(),
        foreignKeys: z.array(foreignKeySchema).optional(),
        checks: z.array(checkConstraintSchema).optional(),
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
  const enumValues =
    typeof column === "string" ? undefined : (column.type === "text" ? column.enum : undefined);
  let validator: z.ZodType<unknown>;

  switch (type) {
    case "text":
    case "timestamp":
      validator = z.string();
      if (type === "text" && enumValues && enumValues.length > 0) {
        validator = z.enum([...enumValues] as [string, ...string[]]);
      }
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

function toJsonSchemaType(column: TableColumnDefinition): Record<string, unknown> {
  const type = readColumnType(column);
  const baseType = type === "integer" ? "number" : type === "boolean" ? "boolean" : "string";
  const enumValues =
    typeof column === "string" ? undefined : (column.type === "text" ? column.enum : undefined);

  if (isColumnNullable(column)) {
    return {
      type: [baseType, "null"],
      ...(enumValues && enumValues.length > 0 ? { enum: [...enumValues, null] } : {}),
    };
  }

  return {
    type: baseType,
    ...(enumValues && enumValues.length > 0 ? { enum: [...enumValues] } : {}),
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

export function buildTableRowsJsonSchema(table: TableDefinition): Record<string, unknown> {
  const columnProperties: Record<string, unknown> = {};
  const required: string[] = [];

  for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
    columnProperties[columnName] = toJsonSchemaType(columnDefinition);
    required.push(columnName);
  }

  return {
    type: "array",
    items: {
      type: "object",
      additionalProperties: false,
      required,
      properties: columnProperties,
    },
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
            maxRows: {
              anyOf: [{ type: "integer", minimum: 0 }, { type: "null" }],
            },
            reject: {
              type: "object",
              additionalProperties: false,
              properties: {
                requiresLimit: { type: "boolean" },
                forbidFullScan: { type: "boolean" },
                requireAnyFilterOn: {
                  type: "array",
                  items: { type: "string" },
                },
              },
            },
            fallback: {
              type: "object",
              additionalProperties: false,
              properties: {
                filters: { type: "string", enum: ["allow_local", "require_pushdown"] },
                sorting: { type: "string", enum: ["allow_local", "require_pushdown"] },
                aggregates: { type: "string", enum: ["allow_local", "require_pushdown"] },
                limitOffset: { type: "string", enum: ["allow_local", "require_pushdown"] },
              },
            },
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
                    filterable: { type: "boolean" },
                    sortable: { type: "boolean" },
                    primaryKey: { type: "boolean" },
                    unique: { type: "boolean" },
                    enum: {
                      type: "array",
                      minItems: 1,
                      items: { type: "string" },
                    },
                    foreignKey: {
                      type: "object",
                      additionalProperties: false,
                      required: ["table", "column"],
                      properties: {
                        table: { type: "string" },
                        column: { type: "string" },
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
                    description: { type: "string" },
                  },
                  allOf: [
                    {
                      not: {
                        type: "object",
                        properties: {
                          primaryKey: { const: true },
                          unique: { const: true },
                        },
                        required: ["primaryKey", "unique"],
                      },
                    },
                  ],
                },
              ],
            },
          },
          query: {
            type: "object",
            additionalProperties: false,
            properties: {
              maxRows: {
                anyOf: [{ type: "integer", minimum: 0 }, { type: "null" }],
              },
              reject: {
                type: "object",
                additionalProperties: false,
                properties: {
                  requiresLimit: { type: "boolean" },
                  forbidFullScan: { type: "boolean" },
                  requireAnyFilterOn: {
                    type: "array",
                    items: { type: "string" },
                  },
                },
              },
              fallback: {
                type: "object",
                additionalProperties: false,
                properties: {
                  filters: { type: "string", enum: ["allow_local", "require_pushdown"] },
                  sorting: { type: "string", enum: ["allow_local", "require_pushdown"] },
                  aggregates: { type: "string", enum: ["allow_local", "require_pushdown"] },
                  limitOffset: { type: "string", enum: ["allow_local", "require_pushdown"] },
                },
              },
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
              checks: {
                type: "array",
                items: {
                  type: "object",
                  additionalProperties: false,
                  required: ["kind", "column", "values"],
                  properties: {
                    kind: { type: "string", enum: ["in"] },
                    column: { type: "string" },
                    values: {
                      type: "array",
                      minItems: 1,
                      items: {
                        anyOf: [
                          { type: "string" },
                          { type: "number" },
                          { type: "boolean" },
                          { type: "null" },
                        ],
                      },
                    },
                    name: { type: "string" },
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
