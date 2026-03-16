import { defineConfig } from "vite-plus";

export default defineConfig({
  ...(process.env.VITEST && process.env.CI ? { cacheDir: "node_modules/.vite-ci/vitest" } : {}),
  resolve: {
    conditions: ["source", "module", "import", "default"],
  },
  test: {
    include: ["**/__tests__/**/*.test.ts"],
    exclude: [
      "**/node_modules/**",
      "**/dist/**",
      "**/.{idea,git,cache,output,temp}/**",
      "test/**",
      "packages/runtime/src/__tests__/compliance/standards-gaps.todo.test.ts",
    ],
    coverage: {
      provider: "v8" as const,
      reporter: ["text", "html", "lcov", "json-summary"],
      reportsDirectory: "coverage",
      exclude: ["**/*.d.ts", "**/*.result-type-inference.ts"],
    },
    testTimeout: 30_000,
  },
  lint: {
    ignorePatterns: ["**/dist/**", "**/node_modules/**"],
    options: {
      typeAware: true,
    },
    categories: {
      correctness: "error",
    },
  },
  fmt: {
    ignorePatterns: ["**/coverage/**", "**/dist/**", "**/node_modules/**"],
  },
  run: {
    cache: {
      scripts: true,
    },
  },
});
