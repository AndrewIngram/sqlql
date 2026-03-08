import {
  CONTEXT_MODULE_ID,
  DEFAULT_CONTEXT_CODE,
  DB_PROVIDER_MODULE_ID,
  DEFAULT_DB_PROVIDER_CODE,
  DEFAULT_GENERATED_DB_FILE_CODE,
  DEFAULT_REDIS_PROVIDER_CODE,
  GENERATED_DB_MODULE_ID,
  REDIS_PROVIDER_MODULE_ID,
} from "./examples";
import type { PlaygroundWorkspaceUserFiles } from "./playground-workspace";

export interface PlaygroundSchemaProgramOptions {
  modules?: Record<string, string>;
}

export function buildPlaygroundModules(
  options: PlaygroundSchemaProgramOptions = {},
): Record<string, string> {
  return {
    [CONTEXT_MODULE_ID]: DEFAULT_CONTEXT_CODE,
    [DB_PROVIDER_MODULE_ID]: DEFAULT_DB_PROVIDER_CODE,
    [GENERATED_DB_MODULE_ID]: DEFAULT_GENERATED_DB_FILE_CODE,
    [REDIS_PROVIDER_MODULE_ID]: DEFAULT_REDIS_PROVIDER_CODE,
    ...options.modules,
  };
}

export function buildPlaygroundWorkspaceFiles(
  schemaCode: string,
  options: PlaygroundSchemaProgramOptions = {},
): PlaygroundWorkspaceUserFiles {
  const modules = buildPlaygroundModules(options);
  return {
    schemaCode,
    contextCode: modules[CONTEXT_MODULE_ID] ?? DEFAULT_CONTEXT_CODE,
    dbProviderCode: modules[DB_PROVIDER_MODULE_ID] ?? DEFAULT_DB_PROVIDER_CODE,
    redisProviderCode: modules[REDIS_PROVIDER_MODULE_ID] ?? DEFAULT_REDIS_PROVIDER_CODE,
    generatedDbCode: modules[GENERATED_DB_MODULE_ID] ?? DEFAULT_GENERATED_DB_FILE_CODE,
  };
}

export function serializeStringRecord(record: Record<string, string>): string {
  return JSON.stringify(
    Object.entries(record).sort(([left], [right]) => left.localeCompare(right)),
  );
}
