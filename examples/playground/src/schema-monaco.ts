import type * as Monaco from "monaco-editor";

import {
  buildPlaygroundWorkspaceSnapshot,
  PLAYGROUND_WORKSPACE_ROOT_URI,
} from "./playground-workspace";

const STATIC_WORKSPACE = buildPlaygroundWorkspaceSnapshot({
  schemaCode: "",
  dbProviderCode: "",
  kvProviderCode: "",
  generatedDbCode: "",
});

let workspaceLibrariesRegistered = false;

function inferLanguage(path: string): "typescript" | "javascript" {
  if (path.endsWith(".ts") || path.endsWith(".d.ts") || path.endsWith(".tsx")) {
    return "typescript";
  }
  return "javascript";
}

export function configureSchemaTypescriptProject(
  monaco: typeof Monaco,
): void {
  monaco.languages.typescript.typescriptDefaults.setCompilerOptions({
    target: monaco.languages.typescript.ScriptTarget.ES2020,
    module: monaco.languages.typescript.ModuleKind.ESNext,
    moduleResolution: monaco.languages.typescript.ModuleResolutionKind.NodeJs,
    strict: true,
    noEmit: true,
    esModuleInterop: true,
    allowSyntheticDefaultImports: true,
    allowNonTsExtensions: true,
    allowImportingTsExtensions: true,
    skipLibCheck: true,
    lib: ["lib.es2021.d.ts", "lib.dom.d.ts"],
    baseUrl: PLAYGROUND_WORKSPACE_ROOT_URI,
    rootDirs: [PLAYGROUND_WORKSPACE_ROOT_URI],
  });
  monaco.languages.typescript.typescriptDefaults.setDiagnosticsOptions({
    noSyntaxValidation: false,
    noSemanticValidation: false,
    noSuggestionDiagnostics: false,
  });
  monaco.languages.typescript.typescriptDefaults.setEagerModelSync(true);

  if (workspaceLibrariesRegistered) {
    return;
  }

  for (const [path, contents] of Object.entries(STATIC_WORKSPACE.sourceFiles)) {
    const uri = monaco.Uri.parse(`file://${path}`);
    const existing = monaco.editor.getModel(uri);
    if (!existing) {
      monaco.editor.createModel(contents, inferLanguage(path), uri);
    }
  }

  for (const [path, contents] of Object.entries(STATIC_WORKSPACE.declarationFiles)) {
    monaco.languages.typescript.typescriptDefaults.addExtraLib(
      contents,
      `file://${path}`,
    );
  }

  workspaceLibrariesRegistered = true;
}
