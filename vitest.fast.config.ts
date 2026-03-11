import { configDefaults, defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

import { sharedCoverageConfig, SLOW_PLAYGROUND_TEST_FILES } from "./vitest.config";

const rootDir = fileURLToPath(new URL(".", import.meta.url));
const workspaceAliases = [
  {
    find: /^@tupl\/foundation$/,
    replacement: resolve(rootDir, "packages/foundation/src/index.ts"),
  },
  {
    find: /^@tupl\/schema$/,
    replacement: resolve(rootDir, "packages/schema/src/index.ts"),
  },
  {
    find: /^@tupl\/provider-kit\/shapes$/,
    replacement: resolve(rootDir, "packages/provider-kit/src/provider/shapes/index.ts"),
  },
  {
    find: /^@tupl\/provider-kit$/,
    replacement: resolve(rootDir, "packages/provider-kit/src/index.ts"),
  },
  {
    find: /^@tupl\/schema-model$/,
    replacement: resolve(rootDir, "packages/schema-model/src/index.ts"),
  },
  {
    find: /^@tupl\/planner$/,
    replacement: resolve(rootDir, "packages/planner/src/index.ts"),
  },
  {
    find: /^@tupl\/runtime\/executor$/,
    replacement: resolve(rootDir, "packages/runtime/src/runtime/executor.ts"),
  },
  {
    find: /^@tupl\/runtime$/,
    replacement: resolve(rootDir, "packages/runtime/src/index.ts"),
  },
  {
    find: /^@tupl\/provider-drizzle$/,
    replacement: resolve(rootDir, "packages/provider-drizzle/src/index.ts"),
  },
  {
    find: /^@tupl\/provider-ioredis$/,
    replacement: resolve(rootDir, "packages/provider-ioredis/src/index.ts"),
  },
  {
    find: /^@tupl\/provider-objection$/,
    replacement: resolve(rootDir, "packages/provider-objection/src/index.ts"),
  },
  {
    find: /^@tupl\/provider-kysely$/,
    replacement: resolve(rootDir, "packages/provider-kysely/src/index.ts"),
  },
] as const;

export default defineConfig({
  cacheDir: "node_modules/.vite-fast",
  resolve: {
    alias: workspaceAliases,
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
    coverage: sharedCoverageConfig,
  },
});
