import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  resolve: {
    tsconfigPaths: true,
  },
  worker: {
    format: "es" as const,
  },
  server: {
    host: true,
    port: 5174,
  },
});
