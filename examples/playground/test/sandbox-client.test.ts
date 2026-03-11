import { afterEach, describe, expect, it, vi } from "vitest";

describe("playground sandbox client", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.resetModules();
    vi.doUnmock("../src/playground-sandbox");
    delete (globalThis as typeof globalThis & { Worker?: typeof Worker }).Worker;
  });

  it("falls back to in-process execution when the worker request cannot be posted", async () => {
    const validateSchemaInSandbox = vi.fn().mockResolvedValue({
      ok: false,
      issues: [{ severity: "error", message: "SCHEMA_EXPORT_MISSING" }],
      diagnostics: [],
      graph: null,
      schemaText: "",
      exports: [],
    });

    vi.doMock("../src/playground-sandbox", () => {
      return {
        validateSchemaInSandbox,
        createSandboxSession: vi.fn(),
        disposeSandboxSession: vi.fn(),
        nextSandboxSessionEvent: vi.fn(),
        replaySandboxSession: vi.fn(),
        runSandboxSessionToCompletion: vi.fn(),
      };
    });

    class ThrowingWorker {
      onmessage: ((event: MessageEvent<unknown>) => void) | null = null;
      onerror: ((event: ErrorEvent) => void) | null = null;
      onmessageerror: ((event: MessageEvent<unknown>) => void) | null = null;

      postMessage(): void {
        throw new DOMException("Could not clone request.", "DataCloneError");
      }

      terminate(): void {}
    }

    (
      globalThis as typeof globalThis & {
        Worker?: new (url: URL | string, options?: WorkerOptions) => Worker;
      }
    ).Worker = ThrowingWorker as unknown as new (
      url: URL | string,
      options?: WorkerOptions,
    ) => Worker;

    const { requestSandboxWorker } = await import("../src/playground-sandbox-client");
    const result = await requestSandboxWorker("validate_schema", {
      schemaCode: "",
      options: {},
    });

    expect(validateSchemaInSandbox).toHaveBeenCalledWith("", {});
    expect(result.ok).toBe(false);
    expect(result.issues[0]?.message).toContain("SCHEMA_EXPORT_MISSING");
  });
});
