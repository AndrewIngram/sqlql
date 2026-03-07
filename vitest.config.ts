import { defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

const rootDir = fileURLToPath(new URL(".", import.meta.url));

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
      "examples/**/test/**/*.test.ts",
    ],
    coverage: {
      reporter: ["text", "html"],
    },
  },
});
