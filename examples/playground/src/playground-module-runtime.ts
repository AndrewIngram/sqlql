import * as ts from "typescript";

import type { PlaygroundWorkspaceSnapshot } from "./playground-workspace";

export interface VirtualModuleRuntimeOptions {
  workspace: PlaygroundWorkspaceSnapshot;
  externalModules?: Record<string, unknown>;
}

export interface VirtualModuleRuntime {
  executeModule: (entryPath: string) => Record<string, unknown>;
}

function normalizePath(path: string): string {
  const normalized = path.replace(/\\/gu, "/");
  const compact = normalized.replace(/\/+/gu, "/");
  if (compact === "") {
    return "/";
  }
  return compact.startsWith("/") ? compact : `/${compact}`;
}

function dirname(path: string): string {
  const normalized = normalizePath(path);
  const index = normalized.lastIndexOf("/");
  if (index <= 0) {
    return "/";
  }
  return normalized.slice(0, index);
}

function resolveRelativePath(fromFile: string, specifier: string): string {
  const baseSegments = dirname(fromFile).split("/").filter(Boolean);
  const relativeSegments = specifier.split("/");
  for (const segment of relativeSegments) {
    if (segment === "" || segment === ".") {
      continue;
    }
    if (segment === "..") {
      baseSegments.pop();
      continue;
    }
    baseSegments.push(segment);
  }
  return normalizePath(`/${baseSegments.join("/")}`);
}

function splitBareSpecifier(specifier: string): { packageName: string; subpath: string | null } {
  if (specifier.startsWith("@")) {
    const [scope, name, ...rest] = specifier.split("/");
    return {
      packageName: `${scope ?? ""}/${name ?? ""}`,
      subpath: rest.length > 0 ? rest.join("/") : null,
    };
  }
  const [name, ...rest] = specifier.split("/");
  return {
    packageName: name ?? "",
    subpath: rest.length > 0 ? rest.join("/") : null,
  };
}

function resolveWorkspaceFile(allFiles: Record<string, string>, basePath: string): string | null {
  const exact = normalizePath(basePath);
  const candidates = [
    exact,
    `${exact}.ts`,
    `${exact}.tsx`,
    `${exact}.js`,
    `${exact}.mjs`,
    `${exact}/index.ts`,
    `${exact}/index.tsx`,
    `${exact}/index.js`,
    `${exact}/index.mjs`,
  ];

  for (const candidate of candidates) {
    if (candidate in allFiles) {
      return candidate;
    }
  }

  return null;
}

function resolveModulePath(
  workspace: PlaygroundWorkspaceSnapshot,
  externalModules: Record<string, unknown>,
  fromFile: string,
  specifier: string,
): { kind: "external"; id: string } | { kind: "workspace"; path: string } {
  if (specifier in externalModules) {
    return {
      kind: "external",
      id: specifier,
    };
  }

  const allFiles = workspace.allFiles;
  if (specifier.startsWith(".") || specifier.startsWith("/")) {
    const resolved = resolveWorkspaceFile(
      allFiles,
      specifier.startsWith(".") ? resolveRelativePath(fromFile, specifier) : specifier,
    );
    if (!resolved) {
      throw new Error(`Unsupported import in playground module graph: ${specifier}`);
    }
    return {
      kind: "workspace",
      path: resolved,
    };
  }

  const { packageName, subpath } = splitBareSpecifier(specifier);
  const packageRoot = `${workspace.rootPath}/node_modules/${packageName}`;
  const bareBase = subpath ? `${packageRoot}/${subpath}` : packageRoot;
  const resolved = resolveWorkspaceFile(allFiles, bareBase);
  if (!resolved) {
    throw new Error(`Unsupported import in playground module graph: ${specifier}`);
  }

  return {
    kind: "workspace",
    path: resolved,
  };
}

function transpileWorkspaceModuleOrThrow(source: string, filePath: string): string {
  const transpiled = ts.transpileModule(source, {
    compilerOptions: {
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.CommonJS,
      moduleResolution: ts.ModuleResolutionKind.NodeJs,
      strict: true,
      esModuleInterop: true,
      allowImportingTsExtensions: true,
    },
    reportDiagnostics: true,
    fileName: filePath,
  });

  const firstError = (transpiled.diagnostics ?? []).find(
    (diagnostic) => diagnostic.category === ts.DiagnosticCategory.Error,
  );
  if (firstError) {
    const message = ts.flattenDiagnosticMessageText(firstError.messageText, "\n");
    throw new Error(`[TS_PARSE_ERROR] ${filePath}: ${message}`);
  }

  return transpiled.outputText;
}

export function createVirtualModuleRuntime(
  options: VirtualModuleRuntimeOptions,
): VirtualModuleRuntime {
  const workspace = options.workspace;
  const externalModules = options.externalModules ?? {};
  const cache = new Map<string, Record<string, unknown>>();

  const executeResolvedPath = (path: string): Record<string, unknown> => {
    const cached = cache.get(path);
    if (cached) {
      return cached;
    }

    const source = workspace.allFiles[path];
    if (typeof source !== "string") {
      throw new Error(`Unknown workspace module: ${path}`);
    }

    const moduleRecord: { exports: Record<string, unknown> } = {
      exports: {},
    };
    cache.set(path, moduleRecord.exports);

    const require = (specifier: string): unknown => {
      const resolved = resolveModulePath(workspace, externalModules, path, specifier);
      if (resolved.kind === "external") {
        return externalModules[resolved.id];
      }
      return executeResolvedPath(resolved.path);
    };

    const transpiled = transpileWorkspaceModuleOrThrow(source, path);
    // oxlint-disable-next-line typescript-eslint/no-implied-eval -- The playground executes transpiled in-memory modules intentionally.
    const runner = new Function(
      "exports",
      "module",
      "require",
      `${transpiled}\n//# sourceURL=file://${path}`,
    ) as (
      exports: Record<string, unknown>,
      module: { exports: Record<string, unknown> },
      requireFn: (specifier: string) => unknown,
    ) => void;

    runner(moduleRecord.exports, moduleRecord, require);
    cache.set(path, moduleRecord.exports);
    return moduleRecord.exports;
  };

  return {
    executeModule: executeResolvedPath,
  };
}

export function executeVirtualModule(
  entryPath: string,
  options: VirtualModuleRuntimeOptions,
): Record<string, unknown> {
  return createVirtualModuleRuntime(options).executeModule(entryPath);
}
