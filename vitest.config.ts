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
    include: ["test/**/*.test.ts", "examples/**/test/**/*.test.ts"],
    coverage: {
      reporter: ["text", "html"],
    },
  },
});
