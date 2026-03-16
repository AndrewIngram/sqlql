import { defineConfig } from "vite-plus";

export default defineConfig({
  resolve: {
    tsconfigPaths: true,
  },
  test: {
    include: [
      "test/providers/__tests__/**/*.test.ts",
      "packages/*/src/**/__tests__/**/*.test.ts",
      "examples/playground/__tests__/**/*.test.ts",
    ],
    exclude: [
      "**/node_modules/**",
      "**/dist/**",
      "**/.{idea,git,cache,output,temp}/**",
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
