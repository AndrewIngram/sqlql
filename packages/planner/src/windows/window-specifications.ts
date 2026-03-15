import type { WindowClauseEntryAst, WindowSpecificationAst } from "../sqlite-parser/ast";
import type { RelWindowFrame } from "@tupl/foundation";

/**
 * Window specifications own named-window resolution and supported frame parsing.
 */
export function parseNamedWindowSpecifications(
  entries: WindowClauseEntryAst[] | undefined,
): Map<string, WindowSpecificationAst> {
  const out = new Map<string, WindowSpecificationAst>();
  for (const entry of entries ?? []) {
    const spec = entry.as_window_specification?.window_specification;
    if (!spec) {
      continue;
    }
    out.set(entry.name, spec);
  }
  return out;
}

export function parseWindowOver(
  over: unknown,
  windowDefinitions: Map<string, WindowSpecificationAst>,
): WindowSpecificationAst | null {
  if (!over || typeof over !== "object") {
    return null;
  }

  const rawSpec = (over as { as_window_specification?: unknown }).as_window_specification;
  if (!rawSpec) {
    return null;
  }

  if (typeof rawSpec === "string") {
    const resolved = windowDefinitions.get(rawSpec);
    if (!resolved) {
      return null;
    }
    return resolved;
  }

  if (typeof rawSpec !== "object") {
    return null;
  }

  const spec = (rawSpec as { window_specification?: unknown }).window_specification;
  if (!spec || typeof spec !== "object") {
    return null;
  }
  return spec as WindowSpecificationAst;
}

export function parseWindowFrameClause(raw: string | undefined): RelWindowFrame | null {
  if (!raw) {
    return null;
  }

  const normalized = raw.replace(/\s+/g, " ").trim().toUpperCase();
  const match = normalized.match(/^(ROWS|RANGE|GROUPS) BETWEEN (.+) AND (.+)$/);
  if (!match) {
    return null;
  }

  const rawMode = match[1];
  const rawStart = match[2];
  const rawEnd = match[3];
  if (!rawMode || !rawStart || !rawEnd) {
    return null;
  }
  const start = parseWindowFrameBound(rawStart);
  const end = parseWindowFrameBound(rawEnd);
  if (!start || !end) {
    return null;
  }

  return {
    mode: rawMode.toLowerCase() as RelWindowFrame["mode"],
    start,
    end,
  };
}

function parseWindowFrameBound(raw: string): RelWindowFrame["start"] | null {
  switch (raw) {
    case "UNBOUNDED PRECEDING":
      return { kind: "unbounded_preceding" };
    case "CURRENT ROW":
      return { kind: "current_row" };
    case "UNBOUNDED FOLLOWING":
      return { kind: "unbounded_following" };
  }

  const preceding = raw.match(/^(\d+) PRECEDING$/);
  if (preceding) {
    return {
      kind: "preceding",
      offset: Number(preceding[1]),
    };
  }

  const following = raw.match(/^(\d+) FOLLOWING$/);
  if (following) {
    return {
      kind: "following",
      offset: Number(following[1]),
    };
  }

  return null;
}
