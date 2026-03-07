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
      "test/parser/**/*.test.ts",
      "test/query/*runtime.test.ts",
      "test/query/session.test.ts",
      "test/providers/**/*.test.ts",
      "examples/**/test/**/*.test.ts",
    ],
    exclude: [...configDefaults.exclude, ...SLOW_PLAYGROUND_TEST_FILES],
    coverage: {
      reporter: ["text", "html"],
    },
  },
});
