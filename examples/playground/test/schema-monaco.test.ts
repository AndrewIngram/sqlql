import { describe, expect, it, vi } from "vitest";

import { configureSchemaTypescriptProject } from "../src/schema-monaco";

describe("playground/schema-monaco", () => {
  it("applies TypeScript project config and registers workspace libraries", () => {
    const setCompilerOptions = vi.fn();
    const setDiagnosticsOptions = vi.fn();
    const setEagerModelSync = vi.fn();
    const addExtraLib = vi.fn();
    const createModel = vi.fn();
    const getModel = vi.fn(() => null);

    const monaco = {
      Uri: {
        parse: (value: string) => ({ toString: () => value }),
      },
      editor: {
        createModel,
        getModel,
      },
      languages: {
        typescript: {
          ScriptTarget: {
            ES2020: "ES2020",
          },
          ModuleKind: {
            ESNext: "ESNext",
          },
          ModuleResolutionKind: {
            NodeJs: "NodeJs",
          },
          typescriptDefaults: {
            setCompilerOptions,
            setDiagnosticsOptions,
            setEagerModelSync,
            addExtraLib,
          },
        },
      },
    } as unknown as Parameters<typeof configureSchemaTypescriptProject>[0];

    configureSchemaTypescriptProject(monaco);

    expect(setCompilerOptions).toHaveBeenCalledTimes(1);
    expect(setCompilerOptions).toHaveBeenCalledWith(
      expect.objectContaining({
        strict: true,
        noEmit: true,
        baseUrl: "file:///playground/workspace",
      }),
    );
    expect(setDiagnosticsOptions).toHaveBeenCalledWith({
      noSyntaxValidation: false,
      noSemanticValidation: false,
      noSuggestionDiagnostics: false,
    });
    expect(setEagerModelSync).toHaveBeenCalledWith(true);
    expect(createModel).toHaveBeenCalled();
    expect(addExtraLib).toHaveBeenCalled();
  });
});
