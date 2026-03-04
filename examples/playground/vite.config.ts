import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

const rootDir = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig(({ mode }) => {
  const isVitest = process.env.VITEST === "true" || mode === "test";

  return {
    plugins: [react()],
    resolve: {
      alias: {
        "@": resolve(rootDir, "./src"),
        sqlql: resolve(rootDir, "../../src/index.ts"),
        "@sqlql/drizzle": resolve(rootDir, "../../packages/drizzle/src/index.ts"),
        ...(!isVitest
          ? { "@electric-sql/pglite": resolve(rootDir, "./src/pglite-browser-shim.ts") }
          : {}),
      },
    },
    server: {
      host: true,
      port: 5174,
    },
  };
});
