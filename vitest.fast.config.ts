import { configDefaults, defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

import { SLOW_PLAYGROUND_TEST_FILES } from "./vitest.config";

const rootDir = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig({
  cacheDir: "node_modules/.vite-fast",
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
      "test/compliance/*.test.ts",
      "test/query/*.test.ts",
      "test/providers/*.test.ts",
      "examples/playground/test/**/*.test.ts",
    ],
    exclude: [
      ...configDefaults.exclude,
      ...SLOW_PLAYGROUND_TEST_FILES,
      "test/compliance/standards-gaps.todo.test.ts",
    ],
    coverage: {
      reporter: ["text", "html"],
    },
  },
});
