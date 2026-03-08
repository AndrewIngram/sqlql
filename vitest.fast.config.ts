import { configDefaults, defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

import { SLOW_PLAYGROUND_TEST_FILES } from "./vitest.config";

const rootDir = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig({
  cacheDir: "node_modules/.vite-fast",
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
      ...configDefaults.exclude,
      ...SLOW_PLAYGROUND_TEST_FILES,
      "test/compliance/standards-gaps.todo.test.ts",
    ],
    coverage: {
      reporter: ["text", "html"],
    },
  },
});
