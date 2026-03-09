import { configDefaults, defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

import { SLOW_PLAYGROUND_TEST_FILES } from "./vitest.config";

const rootDir = fileURLToPath(new URL(".", import.meta.url));
const workspaceAliases = [
  {
    find: /^@tupl\/core\/provider\/shapes$/,
    replacement: resolve(rootDir, "packages/core/src/provider/shapes/index.ts"),
  },
  {
    find: /^@tupl\/core\/provider$/,
    replacement: resolve(rootDir, "packages/core/src/provider/index.ts"),
  },
  {
    find: /^@tupl\/core\/schema$/,
    replacement: resolve(rootDir, "packages/core/src/schema/index.ts"),
  },
  {
    find: /^@tupl\/core\/planner$/,
    replacement: resolve(rootDir, "packages/core/src/planner/index.ts"),
  },
  {
    find: /^@tupl\/core\/model\/rel$/,
    replacement: resolve(rootDir, "packages/core/src/model/rel.ts"),
  },
  {
    find: /^@tupl\/core\/runtime\/executor$/,
    replacement: resolve(rootDir, "packages/core/src/runtime/executor.ts"),
  },
  {
    find: /^@tupl\/core$/,
    replacement: resolve(rootDir, "packages/core/src/index.ts"),
  },
  {
    find: /^@tupl\/schema$/,
    replacement: resolve(rootDir, "packages/schema/src/index.ts"),
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
  {
    find: /^@tupl-internal\/provider\/shapes$/,
    replacement: resolve(rootDir, "packages/internal-provider/src/shapes/index.ts"),
  },
  {
    find: /^@tupl-internal\/provider$/,
    replacement: resolve(rootDir, "packages/internal-provider/src/index.ts"),
  },
  {
    find: /^@tupl-internal\/foundation$/,
    replacement: resolve(rootDir, "packages/internal-foundation/src/index.ts"),
  },
  {
    find: /^@tupl-internal\/schema$/,
    replacement: resolve(rootDir, "packages/internal-schema/src/index.ts"),
  },
  {
    find: /^@tupl-internal\/planner$/,
    replacement: resolve(rootDir, "packages/internal-planner/src/index.ts"),
  },
  {
    find: /^@tupl-internal\/runtime\/executor$/,
    replacement: resolve(rootDir, "packages/internal-runtime/src/executor.ts"),
  },
  {
    find: /^@tupl-internal\/runtime$/,
    replacement: resolve(rootDir, "packages/internal-runtime/src/index.ts"),
  },
] as const;

export default defineConfig({
  cacheDir: "node_modules/.vite-playground-slow",
  resolve: {
    alias: workspaceAliases,
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
