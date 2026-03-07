import * as ts from "typescript";
import { describe, expect, it } from "vitest";

import {
  DEFAULT_DB_PROVIDER_CODE,
  DEFAULT_FACADE_SCHEMA_CODE,
  DEFAULT_GENERATED_DB_FILE_CODE,
  DEFAULT_KV_PROVIDER_CODE,
} from "../src/examples";
import {
  buildPlaygroundWorkspaceSnapshot,
  PLAYGROUND_DB_PROVIDER_FILE_PATH,
  PLAYGROUND_GENERATED_DB_FILE_PATH,
  PLAYGROUND_KV_PROVIDER_FILE_PATH,
  PLAYGROUND_SCHEMA_FILE_PATH,
} from "../src/playground-workspace";

const PLAYGROUND_TYPED_FILE_PATHS = new Set<string>([
  PLAYGROUND_SCHEMA_FILE_PATH,
  PLAYGROUND_DB_PROVIDER_FILE_PATH,
  PLAYGROUND_KV_PROVIDER_FILE_PATH,
  PLAYGROUND_GENERATED_DB_FILE_PATH,
]);

function normalizePath(value: string): string {
  return value.replace(/\\/gu, "/");
}

function collectVirtualDirectories(files: Iterable<string>): Set<string> {
  const directories = new Set<string>();
  for (const file of files) {
    const normalized = normalizePath(file);
    let current = normalized.slice(0, normalized.lastIndexOf("/"));
    while (current.length > 0) {
      directories.add(current);
      current = current.slice(0, current.lastIndexOf("/"));
    }
  }
  directories.add("/");
  return directories;
}

function formatDiagnostic(diagnostic: ts.Diagnostic): string {
  const message = ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n");
  if (!diagnostic.file || diagnostic.start == null) {
    return message;
  }

  const position = diagnostic.file.getLineAndCharacterOfPosition(diagnostic.start);
  return `${normalizePath(diagnostic.file.fileName)}:${position.line + 1}:${position.character + 1} ${message}`;
}

function collectWorkspaceDiagnostics(): string[] {
  const workspace = buildPlaygroundWorkspaceSnapshot({
    schemaCode: DEFAULT_FACADE_SCHEMA_CODE,
    dbProviderCode: DEFAULT_DB_PROVIDER_CODE,
    kvProviderCode: DEFAULT_KV_PROVIDER_CODE,
    generatedDbCode: DEFAULT_GENERATED_DB_FILE_CODE,
  });
  const virtualFiles = new Map(
    Object.entries(workspace.allFiles).map(([path, contents]) => [normalizePath(path), contents]),
  );
  const virtualDirectories = collectVirtualDirectories(virtualFiles.keys());

  const options: ts.CompilerOptions = {
    target: ts.ScriptTarget.ES2021,
    module: ts.ModuleKind.ESNext,
    moduleResolution: ts.ModuleResolutionKind.NodeJs,
    strict: true,
    noEmit: true,
    esModuleInterop: true,
    allowSyntheticDefaultImports: true,
    allowNonTsExtensions: true,
    allowImportingTsExtensions: true,
    skipLibCheck: true,
    baseUrl: workspace.rootPath,
    lib: ["lib.es2021.d.ts", "lib.dom.d.ts"],
  };

  const baseHost = ts.createCompilerHost(options, true);
  const host: ts.CompilerHost = {
    ...baseHost,
    getCurrentDirectory: () => workspace.rootPath,
    fileExists: (fileName) => {
      const normalized = normalizePath(fileName);
      return virtualFiles.has(normalized) || baseHost.fileExists(normalized);
    },
    readFile: (fileName) => {
      const normalized = normalizePath(fileName);
      return virtualFiles.get(normalized) ?? baseHost.readFile(normalized);
    },
    getSourceFile: (fileName, languageVersion, onError, shouldCreateNewSourceFile) => {
      const normalized = normalizePath(fileName);
      const sourceText = virtualFiles.get(normalized);
      if (typeof sourceText === "string") {
        return ts.createSourceFile(normalized, sourceText, languageVersion, true);
      }
      return baseHost.getSourceFile(normalized, languageVersion, onError, shouldCreateNewSourceFile);
    },
    directoryExists: (directoryName) => {
      const normalized = normalizePath(directoryName);
      return virtualDirectories.has(normalized) || (baseHost.directoryExists?.(normalized) ?? false);
    },
    getDirectories: (directoryName) => {
      const normalized = normalizePath(directoryName);
      const virtualChildren = [...virtualDirectories]
        .filter((candidate) => candidate.startsWith(`${normalized}/`))
        .map((candidate) => candidate.slice(normalized.length + 1).split("/")[0])
        .filter((segment): segment is string => typeof segment === "string" && segment.length > 0);
      const actualChildren = baseHost.getDirectories?.(normalized) ?? [];
      return [...new Set([...actualChildren, ...virtualChildren])];
    },
    realpath: (path) => normalizePath(path),
  };

  const rootNames = [
    PLAYGROUND_SCHEMA_FILE_PATH,
    PLAYGROUND_DB_PROVIDER_FILE_PATH,
    PLAYGROUND_KV_PROVIDER_FILE_PATH,
    PLAYGROUND_GENERATED_DB_FILE_PATH,
  ];
  const program = ts.createProgram({
    rootNames,
    options,
    host,
  });

  return ts
    .getPreEmitDiagnostics(program)
    .filter((diagnostic) => {
      const fileName = diagnostic.file ? normalizePath(diagnostic.file.fileName) : "";
      return PLAYGROUND_TYPED_FILE_PATHS.has(fileName);
    })
    .map(formatDiagnostic);
}

describe("playground/workspace-typecheck", () => {
  it("typechecks the default virtual workspace", { timeout: 15_000 }, () => {
    expect(collectWorkspaceDiagnostics()).toEqual([]);
  });
});
