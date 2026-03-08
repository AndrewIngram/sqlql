/// <reference types="vite/client" />

const WORKSPACE_ROOT_PATH = "/playground/workspace";
const NODE_MODULES_ROOT_PATH = `${WORKSPACE_ROOT_PATH}/node_modules`;

export const PLAYGROUND_WORKSPACE_ROOT_URI = `file://${WORKSPACE_ROOT_PATH}`;
export const PLAYGROUND_SCHEMA_FILE_PATH = `${WORKSPACE_ROOT_PATH}/schema.ts`;
export const PLAYGROUND_CONTEXT_FILE_PATH = `${WORKSPACE_ROOT_PATH}/context.ts`;
export const PLAYGROUND_DB_PROVIDER_FILE_PATH = `${WORKSPACE_ROOT_PATH}/db-provider.ts`;
export const PLAYGROUND_REDIS_PROVIDER_FILE_PATH = `${WORKSPACE_ROOT_PATH}/redis-provider.ts`;
export const PLAYGROUND_GENERATED_DB_FILE_PATH = `${WORKSPACE_ROOT_PATH}/generated-db.ts`;

export const PLAYGROUND_SCHEMA_FILE_URI = `file://${PLAYGROUND_SCHEMA_FILE_PATH}`;
export const PLAYGROUND_CONTEXT_FILE_URI = `file://${PLAYGROUND_CONTEXT_FILE_PATH}`;
export const PLAYGROUND_DB_PROVIDER_FILE_URI = `file://${PLAYGROUND_DB_PROVIDER_FILE_PATH}`;
export const PLAYGROUND_REDIS_PROVIDER_FILE_URI = `file://${PLAYGROUND_REDIS_PROVIDER_FILE_PATH}`;
export const PLAYGROUND_GENERATED_DB_FILE_URI = `file://${PLAYGROUND_GENERATED_DB_FILE_PATH}`;

export interface PlaygroundWorkspaceUserFiles {
  schemaCode: string;
  contextCode: string;
  dbProviderCode: string;
  redisProviderCode: string;
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

const CORE_SOURCE_IMPORTS = import.meta.glob("../../../packages/core/src/**/*.ts", {
  eager: true,
  import: "default",
  query: "?raw",
}) as Record<string, string>;

const SCHEMA_SOURCE_IMPORTS = import.meta.glob("../../../packages/schema/src/**/*.ts", {
  eager: true,
  import: "default",
  query: "?raw",
}) as Record<string, string>;

const DRIZZLE_SOURCE_IMPORTS = import.meta.glob("../../../packages/provider-drizzle/src/**/*.ts", {
  eager: true,
  import: "default",
  query: "?raw",
}) as Record<string, string>;

const IOREDIS_SOURCE_IMPORTS = import.meta.glob("../../../packages/provider-ioredis/src/**/*.ts", {
  eager: true,
  import: "default",
  query: "?raw",
}) as Record<string, string>;

const DRIZZLE_DECLARATION_IMPORTS = import.meta.glob("../node_modules/drizzle-orm/**/*.d.ts", {
  eager: true,
  import: "default",
  query: "?raw",
}) as Record<string, string>;

const PGLITE_DECLARATION_IMPORTS = import.meta.glob(
  "../node_modules/@electric-sql/pglite/**/*.d.ts",
  {
    eager: true,
    import: "default",
    query: "?raw",
  },
) as Record<string, string>;

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

const CORE_SOURCE_FILES = mapVirtualFiles(
  CORE_SOURCE_IMPORTS,
  "/packages/core/src/",
  `${NODE_MODULES_ROOT_PATH}/@tupl/core`,
);

const SCHEMA_SOURCE_FILES = mapVirtualFiles(
  SCHEMA_SOURCE_IMPORTS,
  "/packages/schema/src/",
  `${NODE_MODULES_ROOT_PATH}/@tupl/schema`,
);

const DRIZZLE_SOURCE_FILES = mapVirtualFiles(
  DRIZZLE_SOURCE_IMPORTS,
  "/packages/provider-drizzle/src/",
  `${NODE_MODULES_ROOT_PATH}/@tupl/provider-drizzle`,
);

const IOREDIS_SOURCE_FILES = mapVirtualFiles(
  IOREDIS_SOURCE_IMPORTS,
  "/packages/provider-ioredis/src/",
  `${NODE_MODULES_ROOT_PATH}/@tupl/provider-ioredis`,
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
import type { IoredisProviderOperation, RedisLike } from "@tupl/provider-ioredis";

export interface PlaygroundIoredisRuntime {
  redis: RedisLike;
  recordOperation?: (operation: IoredisProviderOperation) => void;
}

export declare function getPlaygroundIoredisRuntime(): PlaygroundIoredisRuntime;
`.trim(),
};

const HOST_PACKAGE_SOURCE_FILES: Record<string, string> = {
  [`${NODE_MODULES_ROOT_PATH}/@playground/provider-ioredis-provider-core/index.ts`]: `
export * from "@tupl/provider-ioredis";

import { getPlaygroundIoredisRuntime } from "@playground/runtime";

export const playgroundIoredisRuntime = getPlaygroundIoredisRuntime();
`.trim(),
};

const PGLITE_ROOT_DECLARATION_FILES: Record<string, string> = {
  [`${NODE_MODULES_ROOT_PATH}/@electric-sql/pglite/index.d.ts`]: 'export * from "./dist/index";',
};

const STATIC_SOURCE_FILES = {
  ...CORE_SOURCE_FILES,
  ...SCHEMA_SOURCE_FILES,
  ...DRIZZLE_SOURCE_FILES,
  ...IOREDIS_SOURCE_FILES,
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
    [PLAYGROUND_REDIS_PROVIDER_FILE_PATH]: input.redisProviderCode,
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

export function readPlaygroundWorkspaceUris(workspace: PlaygroundWorkspaceSnapshot): {
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
