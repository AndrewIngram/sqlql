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
      "src/**/__tests__/**/*.test.ts",
      "packages/*/src/**/__tests__/**/*.test.ts",
      "test/compliance/*.test.ts",
      "test/query/*.test.ts",
      "test/providers/*.test.ts",
      "examples/playground/test/**/*.test.ts",
    ],
    exclude: [
      "test/compliance/standards-gaps.todo.test.ts",
    ],
    coverage: {
      reporter: ["text", "html"],
    },
  },
});
