import { defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

const rootDir = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig({
  resolve: {
    alias: {
      "@sqlql/core": resolve(rootDir, "packages/core/src/index.ts"),
      "@sqlql/sql": resolve(rootDir, "packages/sql/src/index.ts"),
      "@sqlql/executor-memory": resolve(rootDir, "packages/executor-memory/src/index.ts"),
      sqlql: resolve(rootDir, "packages/sqlql/src/index.ts"),
    },
  },
  test: {
    include: ["packages/**/test/**/*.test.ts", "examples/**/test/**/*.test.ts"],
    coverage: {
      reporter: ["text", "html"],
    },
  },
});
