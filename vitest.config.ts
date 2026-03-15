import { configDefaults, defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

const rootDir = fileURLToPath(new URL(".", import.meta.url));
const workspaceAliases = [
  {
    find: /^@tupl\/foundation$/,
    replacement: resolve(rootDir, "packages/foundation/src/index.ts"),
  },
  {
    find: /^@tupl\/test-support$/,
    replacement: resolve(rootDir, "packages/test-support/src/index.ts"),
  },
  {
    find: /^@tupl\/test-support\/schema$/,
    replacement: resolve(rootDir, "packages/test-support/src/schema.ts"),
  },
  {
    find: /^@tupl\/test-support\/methods$/,
    replacement: resolve(rootDir, "packages/test-support/src/methods.ts"),
  },
  {
    find: /^@tupl\/test-support\/runtime$/,
    replacement: resolve(rootDir, "packages/test-support/src/runtime.ts"),
  },
  {
    find: /^@tupl\/test-support\/fixtures$/,
    replacement: resolve(rootDir, "packages/test-support/src/fixtures.ts"),
  },
  {
    find: /^@tupl\/schema$/,
    replacement: resolve(rootDir, "packages/schema/src/index.ts"),
  },
  {
    find: /^@tupl\/provider-kit\/testing$/,
    replacement: resolve(rootDir, "packages/provider-kit/src/testing.ts"),
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
    find: /^@tupl\/schema-model\/constraints$/,
    replacement: resolve(rootDir, "packages/schema-model/src/constraints.ts"),
  },
  {
    find: /^@tupl\/schema-model\/ddl$/,
    replacement: resolve(rootDir, "packages/schema-model/src/ddl.ts"),
  },
  {
    find: /^@tupl\/schema-model\/definition$/,
    replacement: resolve(rootDir, "packages/schema-model/src/definition.ts"),
  },
  {
    find: /^@tupl\/schema-model\/enums$/,
    replacement: resolve(rootDir, "packages/schema-model/src/enums.ts"),
  },
  {
    find: /^@tupl\/schema-model\/mapping$/,
    replacement: resolve(rootDir, "packages/schema-model/src/mapping/index.ts"),
  },
  {
    find: /^@tupl\/schema-model\/normalization$/,
    replacement: resolve(rootDir, "packages/schema-model/src/normalization.ts"),
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

const sharedCoverageConfig = {
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
      "test/compliance/*.test.ts",
      "test/query/*.test.ts",
      "test/providers/*.test.ts",
      "examples/playground/test/**/*.test.ts",
    ],
    exclude: [
      ...configDefaults.exclude,
      "packages/runtime/src/__tests__/compliance/standards-gaps.todo.test.ts",
    ],
    coverage: sharedCoverageConfig,
    testTimeout: 30_000,
  },
});
