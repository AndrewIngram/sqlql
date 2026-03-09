import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tsconfigPaths from "vite-tsconfig-paths";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

const rootDir = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig(() => {
  return {
    plugins: [react(), tsconfigPaths()],
    resolve: {
      alias: {
        "@": resolve(rootDir, "./src"),
        "@tupl/core/schema": resolve(rootDir, "../../packages/core/src/schema/index.ts"),
        "@tupl/core/planner": resolve(rootDir, "../../packages/core/src/planner/index.ts"),
        "@tupl/core/provider-shapes": resolve(
          rootDir,
          "../../packages/core/src/provider-shapes/index.ts",
        ),
        "@tupl/core": resolve(rootDir, "../../packages/core/src/index.ts"),
        "@tupl/schema": resolve(rootDir, "../../packages/schema/src/index.ts"),
        "@tupl/provider-drizzle": resolve(rootDir, "../../packages/provider-drizzle/src/index.ts"),
        "@tupl/provider-ioredis": resolve(rootDir, "../../packages/provider-ioredis/src/index.ts"),
        "@tupl/provider-objection": resolve(
          rootDir,
          "../../packages/provider-objection/src/index.ts",
        ),
        "@tupl/provider-kysely": resolve(rootDir, "../../packages/provider-kysely/src/index.ts"),
      },
    },
    worker: {
      format: "es" as const,
    },
    server: {
      host: true,
      port: 5174,
    },
  };
});
