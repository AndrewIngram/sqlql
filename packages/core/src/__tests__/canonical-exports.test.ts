import { describe, expect, it } from "vitest";

import {
  buildLookupOnlyUnsupportedReport as buildPublicLookupOnlyUnsupportedReport,
  canCompileBasicRel as canCompilePublicBasicRel,
} from "@tupl/core/provider/shapes";
import {
  buildLookupOnlyUnsupportedReport as buildInternalLookupOnlyUnsupportedReport,
  canCompileBasicRel as canCompileInternalBasicRel,
} from "@tupl-internal/provider/shapes";
import {
  createExecutableSchema as createPublicExecutableSchema,
  defaultSqlAstParser as publicSqlAstParser,
  lowerSqlToRel as lowerPublicSqlToRel,
} from "@tupl/core";
import { createExecutableSchema as createInternalExecutableSchema } from "@tupl-internal/runtime";
import {
  defaultSqlAstParser as internalSqlAstParser,
  lowerSqlToRel as lowerInternalSqlToRel,
} from "@tupl-internal/planner";

describe("canonical public exports", () => {
  it("re-exports planner bindings from the internal planner package", () => {
    expect(publicSqlAstParser).toBe(internalSqlAstParser);
    expect(lowerPublicSqlToRel).toBe(lowerInternalSqlToRel);
  });

  it("re-exports provider shape helpers from the internal provider package", () => {
    expect(buildPublicLookupOnlyUnsupportedReport).toBe(buildInternalLookupOnlyUnsupportedReport);
    expect(canCompilePublicBasicRel).toBe(canCompileInternalBasicRel);
  });

  it("re-exports executable schema creation from the internal runtime package", () => {
    expect(createPublicExecutableSchema).toBe(createInternalExecutableSchema);
  });
});
