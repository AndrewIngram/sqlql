import { describe, expect, it } from "vitest";

import { EXAMPLE_PACKS, serializeJson } from "../src/examples";
import {
  compilePlaygroundInput,
  createSession,
  replaySession,
  runSessionToCompletion,
} from "../src/session-runtime";

describe("playground/session-replay", () => {
  it("replays to a specific step count deterministically", async () => {
    const pack = EXAMPLE_PACKS[0];
    const query = pack?.queries[0];
    if (!pack || !query) {
      throw new Error("Expected example pack with at least one query.");
    }

    const compiled = compilePlaygroundInput(
      serializeJson(pack.schema),
      serializeJson(pack.rows),
      query.sql,
    );
    if (!compiled.ok) {
      throw new Error(compiled.issues.join("\n"));
    }

    const liveSession = createSession(compiled);
    const first = await liveSession.next();
    if ("done" in first) {
      throw new Error("Expected at least one step event.");
    }

    const replayed = await replaySession(compiled, 1);
    expect(replayed.events).toHaveLength(1);
    expect(replayed.events[0]?.id).toBe(first.id);
  });

  it("runToCompletion helper matches done state and returns rows", async () => {
    const pack = EXAMPLE_PACKS[1];
    const query = pack?.queries[1];
    if (!pack || !query) {
      throw new Error("Expected example pack with at least one query.");
    }

    const compiled = compilePlaygroundInput(
      serializeJson(pack.schema),
      serializeJson(pack.rows),
      query.sql,
    );
    if (!compiled.ok) {
      throw new Error(compiled.issues.join("\n"));
    }

    const session = createSession(compiled);
    const snapshot = await runSessionToCompletion(session, []);

    expect(snapshot.done).toBe(true);
    expect(snapshot.result).not.toBeNull();
    expect((snapshot.result ?? []).length).toBeGreaterThan(0);
  });
});
