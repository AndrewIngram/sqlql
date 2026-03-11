import { describe, expect, it } from "vitest";

import {
  createExecutableSchema as createSchemaExecutableSchema,
  createSchemaBuilder,
} from "@tupl/schema";
import { createExecutableSchema as createRuntimeExecutableSchema } from "@tupl/runtime";
import { createSchemaBuilder as createModelSchemaBuilder } from "@tupl/schema-model";

describe("canonical public exports", () => {
  it("re-exports schema construction from schema-model", () => {
    expect(createSchemaBuilder).toBe(createModelSchemaBuilder);
  });

  it("re-exports executable schema creation from runtime", () => {
    expect(createSchemaExecutableSchema).toBe(createRuntimeExecutableSchema);
  });
});
