import * as ts from "typescript";
import {
  defineSchema,
  type SchemaDefinition,
} from "sqlql";
import * as sqlqlModule from "sqlql";
import type { DrizzleQueryExecutor } from "@sqlql/drizzle";
import * as drizzleAdapterModule from "@sqlql/drizzle";

export type SchemaCodeErrorCode =
  | "TS_PARSE_ERROR"
  | "SCHEMA_EXPORT_MISSING"
  | "SCHEMA_EXPORT_INVALID"
  | "SCHEMA_EXEC_ERROR";

export interface SchemaCodeEvaluationIssue {
  code: SchemaCodeErrorCode;
  message: string;
  line?: number;
  column?: number;
}

export interface SchemaCodeEvaluationSuccess {
  ok: true;
  schema: SchemaDefinition;
}

export interface SchemaCodeEvaluationFailure {
  ok: false;
  issue: SchemaCodeEvaluationIssue;
}

export type SchemaCodeEvaluationResult = SchemaCodeEvaluationSuccess | SchemaCodeEvaluationFailure;

const SCHEMA_FILENAME = "schema.ts";
const ENTRY_MODULE_ID = "__entry__";

export interface SchemaCodeEvaluationOptions {
  modules?: Record<string, string>;
  entryModuleId?: string;
}

function asMessage(value: unknown): string {
  if (value instanceof Error && value.message.length > 0) {
    return value.message;
  }
  if (typeof value === "string" && value.length > 0) {
    return value;
  }
  return "Schema evaluation failed.";
}

function diagnosticToIssue(diagnostic: ts.Diagnostic): SchemaCodeEvaluationIssue {
  const message = ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n");
  if (diagnostic.file && typeof diagnostic.start === "number") {
    const position = diagnostic.file.getLineAndCharacterOfPosition(diagnostic.start);
    return {
      code: "TS_PARSE_ERROR",
      message,
      line: position.line + 1,
      column: position.character + 1,
    };
  }

  return {
    code: "TS_PARSE_ERROR",
    message,
  };
}

function extractSchemaExport(moduleExports: Record<string, unknown>, namedExports: Record<string, unknown>): unknown {
  if ("schema" in moduleExports) {
    return moduleExports.schema;
  }
  if ("schema" in namedExports) {
    return namedExports.schema;
  }
  return undefined;
}

class SchemaEvaluationIssueError extends Error {
  readonly issue: SchemaCodeEvaluationIssue;

  constructor(issue: SchemaCodeEvaluationIssue) {
    super(issue.message);
    this.issue = issue;
  }
}

const schemaEditorDbStub: DrizzleQueryExecutor = {
  select: () => {
    throw new Error("Schema editor DB stub cannot execute queries.");
  },
};

const PGLITE_CDN_URL = "https://cdn.jsdelivr.net/npm/@electric-sql/pglite/dist/index.js";

function transpileModuleOrThrow(source: string, moduleId: string): string {
  const fileName = moduleId === ENTRY_MODULE_ID ? SCHEMA_FILENAME : `${moduleId}.ts`;
  const transpiled = ts.transpileModule(source, {
    compilerOptions: {
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.CommonJS,
      moduleResolution: ts.ModuleResolutionKind.NodeJs,
      strict: true,
      esModuleInterop: true,
    },
    reportDiagnostics: true,
    fileName,
  });

  const diagnostics = (transpiled.diagnostics ?? []).filter(
    (diagnostic) => diagnostic.category === ts.DiagnosticCategory.Error,
  );
  const firstDiagnostic = diagnostics[0];
  if (firstDiagnostic) {
    throw new SchemaEvaluationIssueError(diagnosticToIssue(firstDiagnostic));
  }

  return transpiled.outputText;
}

function createStaticModuleMap(): Record<string, unknown> {
  const createColumnBuilder = () => {
    const builder = {
      primaryKey: () => builder,
      notNull: () => builder,
      references: () => builder,
    };
    return builder;
  };

  return {
    sqlql: sqlqlModule,
    "@sqlql/drizzle": drizzleAdapterModule,
    "@electric-sql/pglite": {
      PGlite: class PGlite {},
    },
    [PGLITE_CDN_URL]: {
      PGlite: class PGlite {},
    },
    "drizzle-orm/pglite": {
      drizzle: () => schemaEditorDbStub,
    },
    "drizzle-orm/pg-core": {
      pgTable: (name: string, columns: Record<string, unknown>) => ({
        _: {
          config: {
            name,
            dialect: "postgres",
          },
        },
        ...columns,
      }),
      text: () => createColumnBuilder(),
      integer: () => createColumnBuilder(),
      boolean: () => createColumnBuilder(),
      timestamp: () => createColumnBuilder(),
    },
  };
}

export function evaluateSchemaCodeInProcess(
  code: string,
  options: SchemaCodeEvaluationOptions = {},
): SchemaCodeEvaluationResult {
  const sourceModules = {
    ...options.modules,
    [(options.entryModuleId ?? ENTRY_MODULE_ID)]: code,
  };
  const staticModules = createStaticModuleMap();
  const moduleCache = new Map<string, Record<string, unknown>>();
  const entryId = options.entryModuleId ?? ENTRY_MODULE_ID;

  const executeModule = (moduleId: string): Record<string, unknown> => {
    const cached = moduleCache.get(moduleId);
    if (cached) {
      return cached;
    }

    const source = sourceModules[moduleId];
    if (typeof source !== "string") {
      throw new Error(`Unsupported import in schema module: ${moduleId}`);
    }

    const transpiledOutput = transpileModuleOrThrow(source, moduleId);
    const moduleRecord: { exports: Record<string, unknown> } = {
      exports: {},
    };
    const exportsRecord: Record<string, unknown> = moduleRecord.exports;

    const require = (id: string): unknown => {
      const fromStatic = staticModules[id];
      if (fromStatic) {
        return fromStatic;
      }
      return executeModule(id);
    };

    const runModule = new Function(
      "exports",
      "module",
      "require",
      `${transpiledOutput}\n//# sourceURL=playground-schema-${moduleId}.js`,
    ) as (exports: Record<string, unknown>, module: { exports: Record<string, unknown> }, requireFn: (id: string) => unknown) => void;
    runModule(exportsRecord, moduleRecord, require);

    const moduleExports = moduleRecord.exports;
    moduleCache.set(moduleId, moduleExports);
    return moduleExports;
  };

  const moduleRecord: { exports: Record<string, unknown> } = {
    exports: {},
  };

  try {
    moduleRecord.exports = executeModule(entryId);
  } catch (error) {
    if (error instanceof SchemaEvaluationIssueError) {
      return {
        ok: false,
        issue: error.issue,
      };
    }
    return {
      ok: false,
      issue: {
        code: "SCHEMA_EXEC_ERROR",
        message: asMessage(error),
      },
    };
  }

  const schemaValue = extractSchemaExport(moduleRecord.exports, moduleRecord.exports);
  if (schemaValue == null) {
    return {
      ok: false,
      issue: {
        code: "SCHEMA_EXPORT_MISSING",
        message: 'Schema module must export `schema` via `export const schema = defineSchema(...)`.',
      },
    };
  }

  try {
    const schema = defineSchema(schemaValue as SchemaDefinition);
    return {
      ok: true,
      schema,
    };
  } catch (error) {
    return {
      ok: false,
      issue: {
        code: "SCHEMA_EXPORT_INVALID",
        message: asMessage(error),
      },
    };
  }
}
