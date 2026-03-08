import { defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

const rootDir = fileURLToPath(new URL(".", import.meta.url));

export const SLOW_PLAYGROUND_TEST_FILES = [
  "examples/playground/test/preset-queries.test.ts",
  "examples/playground/test/provider-pushdown.test.ts",
  "examples/playground/test/session-replay.test.ts",
  "examples/playground/test/validation.test.ts",
  "examples/playground/test/workspace-typecheck.test.ts",
];

export default defineConfig({
  resolve: {
    alias: {
      "@tupl/core": resolve(rootDir, "packages/core/src/index.ts"),
      "@tupl/schema": resolve(rootDir, "packages/schema/src/index.ts"),
      "@tupl/provider-drizzle": resolve(rootDir, "packages/provider-drizzle/src/index.ts"),
      "@tupl/provider-ioredis": resolve(rootDir, "packages/provider-ioredis/src/index.ts"),
      "@tupl/provider-objection": resolve(rootDir, "packages/provider-objection/src/index.ts"),
      "@tupl/provider-kysely": resolve(rootDir, "packages/provider-kysely/src/index.ts"),
    },
  },
  test: {
    include: [
      "src/**/__tests__/**/*.test.ts",
      "packages/*/src/**/__tests__/**/*.test.ts",
      "test/providers/*.test.ts",
      "examples/playground/test/**/*.test.ts",
    ],
    exclude: [
      "packages/core/src/runtime/__tests__/compliance/standards-gaps.todo.test.ts",
    ],
    coverage: {
      reporter: ["text", "html"],
    },
  },
});
