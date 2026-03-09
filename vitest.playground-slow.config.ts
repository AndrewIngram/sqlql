import { configDefaults, defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

import { SLOW_PLAYGROUND_TEST_FILES } from "./vitest.config";

const rootDir = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig({
  cacheDir: "node_modules/.vite-playground-slow",
  resolve: {
    alias: {
      "@tupl/core/schema": resolve(rootDir, "packages/core/src/schema/index.ts"),
      "@tupl/core/planner": resolve(rootDir, "packages/core/src/planner/index.ts"),
      "@tupl/core/provider-shapes": resolve(rootDir, "packages/core/src/provider-shapes/index.ts"),
      "@tupl/core": resolve(rootDir, "packages/core/src/index.ts"),
      "@tupl/schema": resolve(rootDir, "packages/schema/src/index.ts"),
      "@tupl/provider-drizzle": resolve(rootDir, "packages/provider-drizzle/src/index.ts"),
      "@tupl/provider-ioredis": resolve(rootDir, "packages/provider-ioredis/src/index.ts"),
      "@tupl/provider-objection": resolve(rootDir, "packages/provider-objection/src/index.ts"),
      "@tupl/provider-kysely": resolve(rootDir, "packages/provider-kysely/src/index.ts"),
    },
  },
  test: {
    include: SLOW_PLAYGROUND_TEST_FILES,
    exclude: configDefaults.exclude,
    testTimeout: 30_000,
    coverage: {
      reporter: ["text", "html"],
    },
  },
});
