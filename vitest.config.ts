import { defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

const rootDir = fileURLToPath(new URL(".", import.meta.url));
const workspaceAliases = [
  {
    find: /^@tupl\/core\/provider\/shapes$/,
    replacement: resolve(rootDir, "packages/core/src/provider/shapes/index.ts"),
  },
  {
    find: /^@tupl\/core\/provider-shapes$/,
    replacement: resolve(rootDir, "packages/core/src/provider-shapes/index.ts"),
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

export const SLOW_PLAYGROUND_TEST_FILES = [
  "examples/playground/test/preset-queries.test.ts",
  "examples/playground/test/provider-pushdown.test.ts",
  "examples/playground/test/session-replay.test.ts",
  "examples/playground/test/validation.test.ts",
  "examples/playground/test/workspace-typecheck.test.ts",
];

export const sharedCoverageConfig = {
  provider: "v8" as const,
  reporter: ["text", "html", "lcov", "json-summary"] as const,
  reportsDirectory: "coverage",
  exclude: ["**/*.d.ts", "**/*.result-type-inference.ts"],
};

export default defineConfig({
  resolve: {
    alias: workspaceAliases,
  },
  test: {
    include: [
      "src/**/__tests__/**/*.test.ts",
      "packages/*/src/**/__tests__/**/*.test.ts",
      "test/providers/*.test.ts",
      "examples/playground/test/**/*.test.ts",
    ],
    exclude: ["packages/core/src/runtime/__tests__/compliance/standards-gaps.todo.test.ts"],
    coverage: sharedCoverageConfig,
  },
});
