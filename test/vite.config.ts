import { defineConfig } from "vite-plus";

export default defineConfig({
  ...(process.env.VITEST && process.env.CI
    ? { cacheDir: "../node_modules/.vite-ci/workspace-tests" }
    : {}),
  resolve: {
    conditions: ["source", "module", "import", "default"],
  },
  test: {
    include: ["**/__tests__/**/*.test.ts"],
    exclude: [
      "**/node_modules/**",
      "**/dist/**",
      "**/.{idea,git,cache,output,temp}/**",
      "__tests__/package-boundaries.test.ts",
      "__tests__/public-package-imports.test.ts",
    ],
    coverage: {
      provider: "v8" as const,
      reporter: ["text", "html", "lcov", "json-summary"],
      reportsDirectory: "../coverage/workspace-tests",
      exclude: ["**/*.d.ts", "**/*.result-type-inference.ts"],
    },
    testTimeout: 30_000,
  },
});
