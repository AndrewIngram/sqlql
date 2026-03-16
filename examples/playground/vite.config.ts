import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  resolve: {
    conditions: ["source", "module", "import", "default"],
  },
  worker: {
    format: "es" as const,
  },
  server: {
    host: true,
    port: 5174,
  },
});
