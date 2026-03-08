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
