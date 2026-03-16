import { describe, expect, it } from "vite-plus/test";

import {
  DEFAULT_FACADE_SCHEMA_CODE,
  QUERY_PRESETS,
  SCENARIO_PRESETS,
  serializeJson,
} from "../src/examples";
import {
  createSandboxSession,
  nextSandboxSessionEvent,
  runSandboxSessionToCompletion,
} from "../src/playground-sandbox";
import { compilePlaygroundInput } from "../src/session-runtime";

function toSandboxCompiledInput(
  compiled: Extract<Awaited<ReturnType<typeof compilePlaygroundInput>>, { ok: true }>,
) {
  return {
    schemaCode: compiled.schemaCode,
    downstreamRows: compiled.downstreamRows,
    sql: compiled.sql,
    ...(compiled.modules ? { modules: compiled.modules } : {}),
  };
}

describe("playground sandbox session lifecycle", () => {
  it("disposes completed sandbox sessions", { timeout: 20_000 }, async () => {
    const scenario = SCENARIO_PRESETS[0];
    const query = QUERY_PRESETS[0];
    if (!scenario || !query) {
      throw new Error("Expected example pack with at least one query.");
    }

    const compiled = await compilePlaygroundInput(
      DEFAULT_FACADE_SCHEMA_CODE,
      serializeJson(scenario.rows),
      query.sql,
    );
    if (!compiled.ok) {
      throw new Error(compiled.issues.join("\n"));
    }

    const bundle = await createSandboxSession(toSandboxCompiledInput(compiled), scenario.context);
    if (!bundle.ok) {
      throw new Error(bundle.error.message);
    }

    await runSandboxSessionToCompletion(bundle.sessionId);

    await expect(nextSandboxSessionEvent(bundle.sessionId)).rejects.toThrow(
      `Unknown sandbox session: ${bundle.sessionId}`,
    );
  });
});
