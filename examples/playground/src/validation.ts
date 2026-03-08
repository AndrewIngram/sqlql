import { z } from "zod";
import {
  createDataEntityHandle,
  createSchemaBuilder,
  type QueryRow,
  type SchemaDefinition,
  type TableColumnDefinition,
  type TableDefinition,
} from "../../../src/index";

import { DOWNSTREAM_ROWS_SCHEMA } from "./downstream-model";
import { REDIS_INPUT_TABLE_DEFINITION, REDIS_INPUT_TABLE_NAME } from "./redis-provider";
import { requestSandboxWorker } from "./playground-sandbox-client";
import { validateSchemaInSandbox } from "./playground-sandbox";
import type { PlaygroundSchemaProgramOptions } from "./playground-program-files";
import {
  isColumnNullable,
  readColumnType,
  type RowsParseResult,
  type SchemaParseResult,
  type SchemaValidationIssue,
} from "./types";

const sqlScalarTypeSchema = z.enum([
  "text",
  "integer",
  "real",
  "blob",
  "boolean",
  "timestamp",
  "date",
  "datetime",
  "json",
]);
const sqlScalarTypeValues = sqlScalarTypeSchema.options;

const downstreamInputRowsBuilder = createSchemaBuilder<Record<string, never>>();
const inputRowsEntity = (name: string, provider?: string) =>
  createDataEntityHandle({
    entity: name,
    provider: provider ?? "memory",
  });

for (const [tableName, tableDefinition] of Object.entries(DOWNSTREAM_ROWS_SCHEMA.tables)) {
  downstreamInputRowsBuilder.table(
    tableName,
    inputRowsEntity(tableName, tableDefinition.provider),
    {
      columns: tableDefinition.columns,
      ...(tableDefinition.constraints ? { constraints: tableDefinition.constraints } : {}),
    },
  );
}

downstreamInputRowsBuilder.table(
  REDIS_INPUT_TABLE_NAME,
  inputRowsEntity(REDIS_INPUT_TABLE_NAME, REDIS_INPUT_TABLE_DEFINITION.provider),
  {
    columns: REDIS_INPUT_TABLE_DEFINITION.columns,
    ...(REDIS_INPUT_TABLE_DEFINITION.constraints
      ? { constraints: REDIS_INPUT_TABLE_DEFINITION.constraints }
      : {}),
  },
);

const DOWNSTREAM_INPUT_ROWS_SCHEMA: SchemaDefinition = downstreamInputRowsBuilder.build();

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

function canUseWorkerSandbox(): boolean {
  return "window" in globalThis && typeof Worker !== "undefined";
}

export async function parseFacadeSchemaCode(
  value: string,
  options: PlaygroundSchemaProgramOptions = {},
): Promise<SchemaParseResult> {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return {
      ok: false,
      issues: [
        {
          path: "schema.ts",
          message: "[SCHEMA_EXPORT_MISSING] Schema module cannot be empty.",
        },
      ],
    };
  }

  if (canUseWorkerSandbox()) {
    const workerResult = await requestSandboxWorker("validate_schema", {
      schemaCode: value,
      ...(options.modules ? { options: { modules: options.modules } } : {}),
    });
    return workerResult;
  }

  return validateSchemaInSandbox(value, options);
}

export function parseFacadeSchemaText(value: string): SchemaParseResult {
  void value;
  return {
    ok: false,
    issues: [
      {
        path: "$",
        message:
          "Raw facade schema JSON is no longer supported. Export `executableSchema` created via createSchemaBuilder().",
      },
    ],
  };
}

export const parseSchemaText = parseFacadeSchemaText;

function validatorForColumn(column: TableColumnDefinition): z.ZodType<unknown> {
  const type = readColumnType(column);
  const enumValues =
    typeof column === "string" ? undefined : column.type === "text" ? column.enum : undefined;
  let validator: z.ZodType<unknown>;

  switch (type) {
    case "text":
    case "date":
    case "datetime":
    case "timestamp":
      validator = z.string();
      if (type === "text" && enumValues && enumValues.length > 0) {
        validator = z.enum([...enumValues] as [string, ...string[]]);
      }
      break;
    case "integer":
      validator = z.number().finite();
      break;
    case "real":
      validator = z.number().finite();
      break;
    case "boolean":
      validator = z.boolean();
      break;
    case "blob":
      validator = z.instanceof(Uint8Array);
      break;
    case "json":
      validator = z.unknown();
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

export function parseDownstreamRowsText(value: string): RowsParseResult {
  return parseRowsText(DOWNSTREAM_INPUT_ROWS_SCHEMA, value);
}

function toJsonSchemaType(column: TableColumnDefinition): Record<string, unknown> {
  const type = readColumnType(column);
  const baseType = type === "integer" ? "number" : type === "boolean" ? "boolean" : "string";
  const enumValues =
    typeof column === "string" ? undefined : column.type === "text" ? column.enum : undefined;

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
    tables: {
      type: "object",
      additionalProperties: {
        type: "object",
        additionalProperties: false,
        required: ["provider", "columns"],
        properties: {
          provider: { type: "string" },
          columns: {
            type: "object",
            additionalProperties: {
              anyOf: [
                {
                  type: "string",
                  enum: sqlScalarTypeValues,
                },
                {
                  type: "object",
                  additionalProperties: false,
                  required: ["type"],
                  properties: {
                    type: {
                      type: "string",
                      enum: sqlScalarTypeValues,
                    },
                    nullable: { type: "boolean" },
                    primaryKey: { type: "boolean" },
                    unique: { type: "boolean" },
                    enum: {
                      type: "array",
                      minItems: 1,
                      items: { type: "string" },
                    },
                    enumFrom: { type: "string" },
                    enumMap: {
                      type: "object",
                      additionalProperties: { type: "string" },
                    },
                    physicalType: { type: "string" },
                    physicalDialect: {
                      type: "string",
                      enum: ["postgres", "sqlite"],
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
