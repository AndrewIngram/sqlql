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
      sqlql: resolve(rootDir, "src/index.ts"),
    },
  },
  test: {
    include: [
      "test/parser/**/*.test.ts",
      "test/query/*runtime.test.ts",
      "test/query/session.test.ts",
      "test/providers/**/*.test.ts",
      "examples/playground/test/**/*.test.ts",
    ],
    coverage: {
      reporter: ["text", "html"],
    },
  },
});
