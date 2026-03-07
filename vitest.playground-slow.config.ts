import { configDefaults, defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

import { SLOW_PLAYGROUND_TEST_FILES } from "./vitest.config";

const rootDir = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig({
  cacheDir: "node_modules/.vite-playground-slow",
  resolve: {
    alias: {
      sqlql: resolve(rootDir, "src/index.ts"),
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
