/// <reference types="vite/client" />

import kvProviderCoreSourceText from "./kv-provider.ts?raw";

const WORKSPACE_ROOT_PATH = "/playground/workspace";
const NODE_MODULES_ROOT_PATH = `${WORKSPACE_ROOT_PATH}/node_modules`;

export const PLAYGROUND_WORKSPACE_ROOT_URI = `file://${WORKSPACE_ROOT_PATH}`;
export const PLAYGROUND_SCHEMA_FILE_PATH = `${WORKSPACE_ROOT_PATH}/schema.ts`;
export const PLAYGROUND_CONTEXT_FILE_PATH = `${WORKSPACE_ROOT_PATH}/context.ts`;
export const PLAYGROUND_DB_PROVIDER_FILE_PATH = `${WORKSPACE_ROOT_PATH}/db-provider.ts`;
export const PLAYGROUND_KV_PROVIDER_FILE_PATH = `${WORKSPACE_ROOT_PATH}/kv-provider.ts`;
export const PLAYGROUND_GENERATED_DB_FILE_PATH = `${WORKSPACE_ROOT_PATH}/generated-db.ts`;

export const PLAYGROUND_SCHEMA_FILE_URI = `file://${PLAYGROUND_SCHEMA_FILE_PATH}`;
export const PLAYGROUND_CONTEXT_FILE_URI = `file://${PLAYGROUND_CONTEXT_FILE_PATH}`;
export const PLAYGROUND_DB_PROVIDER_FILE_URI = `file://${PLAYGROUND_DB_PROVIDER_FILE_PATH}`;
export const PLAYGROUND_KV_PROVIDER_FILE_URI = `file://${PLAYGROUND_KV_PROVIDER_FILE_PATH}`;
export const PLAYGROUND_GENERATED_DB_FILE_URI = `file://${PLAYGROUND_GENERATED_DB_FILE_PATH}`;

export interface PlaygroundWorkspaceUserFiles {
  schemaCode: string;
  contextCode: string;
  dbProviderCode: string;
  kvProviderCode: string;
  generatedDbCode: string;
}

export interface PlaygroundWorkspaceSnapshot {
  rootPath: string;
  rootUri: string;
  entryPath: string;
  userFiles: Record<string, string>;
  sourceFiles: Record<string, string>;
  declarationFiles: Record<string, string>;
  readonlyPaths: string[];
  allFiles: Record<string, string>;
}

function normalizePath(value: string): string {
  const normalized = value.replace(/\\/gu, "/");
  const compact = normalized.replace(/\/+/gu, "/");
  if (compact === "") {
    return "/";
  }
  if (!compact.startsWith("/")) {
    return `/${compact}`;
  }
  return compact;
}

function toFileUri(path: string): string {
  return `file://${normalizePath(path)}`;
}

function relativeFromMarker(path: string, marker: string): string {
  const normalized = path.replace(/\\/gu, "/");
  const index = normalized.lastIndexOf(marker);
  if (index === -1) {
    throw new Error(`Unable to resolve virtual workspace path for ${path}`);
  }
  return normalized.slice(index + marker.length);
}

function mapVirtualFiles(
  input: Record<string, string>,
  marker: string,
  targetRoot: string,
): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [path, contents] of Object.entries(input)) {
    const relativePath = relativeFromMarker(path, marker);
    out[normalizePath(`${targetRoot}/${relativePath}`)] = contents;
  }
  return out;
}

const SQLQL_SOURCE_IMPORTS = import.meta.glob("../../../src/**/*.ts", {
  eager: true,
  import: "default",
  query: "?raw",
}) as Record<string, string>;

const DRIZZLE_SOURCE_IMPORTS = import.meta.glob("../../../packages/drizzle/src/**/*.ts", {
  eager: true,
  import: "default",
  query: "?raw",
}) as Record<string, string>;

const DRIZZLE_DECLARATION_IMPORTS = import.meta.glob("../node_modules/drizzle-orm/**/*.d.ts", {
  eager: true,
  import: "default",
  query: "?raw",
}) as Record<string, string>;

const PGLITE_DECLARATION_IMPORTS = import.meta.glob("../node_modules/@electric-sql/pglite/**/*.d.ts", {
  eager: true,
  import: "default",
  query: "?raw",
}) as Record<string, string>;

const BETTER_RESULT_DECLARATION_IMPORTS = {
  ...import.meta.glob("../node_modules/better-result/dist/index.d.mts", {
    eager: true,
    import: "default",
    query: "?raw",
  }),
  ...import.meta.glob("../../../node_modules/better-result/dist/index.d.mts", {
    eager: true,
    import: "default",
    query: "?raw",
  }),
} as Record<string, string>;

const MIRRORED_KV_PROVIDER_CORE_SOURCE = kvProviderCoreSourceText.replace(
  /from "\.\.\/\.\.\/\.\.\/src\/index"/gu,
  'from "sqlql"',
);

const SQLQL_SOURCE_FILES = mapVirtualFiles(
  SQLQL_SOURCE_IMPORTS,
  "/src/",
  `${NODE_MODULES_ROOT_PATH}/sqlql`,
);

const DRIZZLE_SOURCE_FILES = mapVirtualFiles(
  DRIZZLE_SOURCE_IMPORTS,
  "/packages/drizzle/src/",
  `${NODE_MODULES_ROOT_PATH}/@sqlql/drizzle`,
);

const DRIZZLE_DECLARATION_FILES = mapVirtualFiles(
  DRIZZLE_DECLARATION_IMPORTS,
  "/node_modules/drizzle-orm/",
  `${NODE_MODULES_ROOT_PATH}/drizzle-orm`,
);

const PGLITE_DECLARATION_FILES = mapVirtualFiles(
  PGLITE_DECLARATION_IMPORTS,
  "/node_modules/@electric-sql/pglite/",
  `${NODE_MODULES_ROOT_PATH}/@electric-sql/pglite`,
);

const BETTER_RESULT_DECLARATION_FILE = Object.values(BETTER_RESULT_DECLARATION_IMPORTS)[0];
if (typeof BETTER_RESULT_DECLARATION_FILE !== "string") {
  throw new Error("Unable to load better-result declarations for playground workspace.");
}

const BETTER_RESULT_DECLARATION_FILES: Record<string, string> = {
  [`${NODE_MODULES_ROOT_PATH}/better-result/index.d.ts`]: BETTER_RESULT_DECLARATION_FILE,
};

const HOST_PACKAGE_DECLARATION_FILES: Record<string, string> = {
  [`${NODE_MODULES_ROOT_PATH}/@playground/runtime/index.d.ts`]: `
export interface PlaygroundKvInputRow {
  key: string;
  value: unknown;
}

export interface PlaygroundKvProviderOperation {
  kind: "kv_lookup";
  provider: string;
  lookup: {
    entity: string;
    op: "scan" | "lookupMany";
    key?: string;
    keys?: unknown[];
  };
  variables: unknown;
}

export interface PlaygroundKvRuntime {
  rows: PlaygroundKvInputRow[];
  recordOperation?: (operation: PlaygroundKvProviderOperation) => void;
}

export declare function getPlaygroundKvRuntime(): PlaygroundKvRuntime;
`.trim(),
};

const HOST_PACKAGE_SOURCE_FILES: Record<string, string> = {
  [`${NODE_MODULES_ROOT_PATH}/@playground/kv-provider-core/core.ts`]:
    MIRRORED_KV_PROVIDER_CORE_SOURCE,
  [`${NODE_MODULES_ROOT_PATH}/@playground/kv-provider-core/index.ts`]: `
export * from "./core";

import { getPlaygroundKvRuntime } from "@playground/runtime";
import type { KvProviderFactoryRuntime } from "./core";

export const playgroundKvRuntime: KvProviderFactoryRuntime = getPlaygroundKvRuntime();
`.trim(),
};

const PGLITE_ROOT_DECLARATION_FILES: Record<string, string> = {
  [`${NODE_MODULES_ROOT_PATH}/@electric-sql/pglite/index.d.ts`]:
    'export * from "./dist/index";',
};

const STATIC_SOURCE_FILES = {
  ...SQLQL_SOURCE_FILES,
  ...DRIZZLE_SOURCE_FILES,
  ...HOST_PACKAGE_SOURCE_FILES,
};

const STATIC_DECLARATION_FILES = {
  ...DRIZZLE_DECLARATION_FILES,
  ...PGLITE_DECLARATION_FILES,
  ...PGLITE_ROOT_DECLARATION_FILES,
  ...BETTER_RESULT_DECLARATION_FILES,
  ...HOST_PACKAGE_DECLARATION_FILES,
};

const STATIC_READONLY_PATHS = [
  ...Object.keys(STATIC_SOURCE_FILES),
  ...Object.keys(STATIC_DECLARATION_FILES),
].sort();

export function buildPlaygroundWorkspaceSnapshot(
  input: PlaygroundWorkspaceUserFiles,
): PlaygroundWorkspaceSnapshot {
  const userFiles: Record<string, string> = {
    [PLAYGROUND_SCHEMA_FILE_PATH]: input.schemaCode,
    [PLAYGROUND_CONTEXT_FILE_PATH]: input.contextCode,
    [PLAYGROUND_DB_PROVIDER_FILE_PATH]: input.dbProviderCode,
    [PLAYGROUND_KV_PROVIDER_FILE_PATH]: input.kvProviderCode,
    [PLAYGROUND_GENERATED_DB_FILE_PATH]: input.generatedDbCode,
  };

  return {
    rootPath: WORKSPACE_ROOT_PATH,
    rootUri: PLAYGROUND_WORKSPACE_ROOT_URI,
    entryPath: PLAYGROUND_SCHEMA_FILE_PATH,
    userFiles,
    sourceFiles: STATIC_SOURCE_FILES,
    declarationFiles: STATIC_DECLARATION_FILES,
    readonlyPaths: STATIC_READONLY_PATHS,
    allFiles: {
      ...STATIC_SOURCE_FILES,
      ...STATIC_DECLARATION_FILES,
      ...userFiles,
    },
  };
}

export function readPlaygroundWorkspaceUris(
  workspace: PlaygroundWorkspaceSnapshot,
): {
  userUris: Record<string, string>;
  readonlyUris: string[];
  sourceUris: string[];
  declarationUris: string[];
} {
  return {
    userUris: Object.fromEntries(
      Object.keys(workspace.userFiles).map((path) => [path, toFileUri(path)]),
    ),
    readonlyUris: workspace.readonlyPaths.map(toFileUri),
    sourceUris: Object.keys(workspace.sourceFiles).map(toFileUri),
    declarationUris: Object.keys(workspace.declarationFiles).map(toFileUri),
  };
}
