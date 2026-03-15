import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tsconfigPaths from "vite-tsconfig-paths";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

const rootDir = fileURLToPath(new URL(".", import.meta.url));
const workspaceAliases = [
  {
    find: /^@tupl\/foundation$/,
    replacement: resolve(rootDir, "../../packages/foundation/src/index.ts"),
  },
  {
    find: /^@tupl\/schema$/,
    replacement: resolve(rootDir, "../../packages/schema/src/index.ts"),
  },
  {
    find: /^@tupl\/provider-kit\/shapes$/,
    replacement: resolve(rootDir, "../../packages/provider-kit/src/provider/shapes/index.ts"),
  },
  {
    find: /^@tupl\/provider-kit$/,
    replacement: resolve(rootDir, "../../packages/provider-kit/src/index.ts"),
  },
  {
    find: /^@tupl\/schema-model$/,
    replacement: resolve(rootDir, "../../packages/schema-model/src/index.ts"),
  },
  {
    find: /^@tupl\/schema-model\/constraints$/,
    replacement: resolve(rootDir, "../../packages/schema-model/src/constraints.ts"),
  },
  {
    find: /^@tupl\/schema-model\/ddl$/,
    replacement: resolve(rootDir, "../../packages/schema-model/src/ddl.ts"),
  },
  {
    find: /^@tupl\/schema-model\/definition$/,
    replacement: resolve(rootDir, "../../packages/schema-model/src/definition.ts"),
  },
  {
    find: /^@tupl\/schema-model\/enums$/,
    replacement: resolve(rootDir, "../../packages/schema-model/src/enums.ts"),
  },
  {
    find: /^@tupl\/schema-model\/mapping$/,
    replacement: resolve(rootDir, "../../packages/schema-model/src/mapping/index.ts"),
  },
  {
    find: /^@tupl\/schema-model\/normalization$/,
    replacement: resolve(rootDir, "../../packages/schema-model/src/normalization.ts"),
  },
  {
    find: /^@tupl\/planner$/,
    replacement: resolve(rootDir, "../../packages/planner/src/index.ts"),
  },
  {
    find: /^@tupl\/runtime\/executor$/,
    replacement: resolve(rootDir, "../../packages/runtime/src/runtime/executor.ts"),
  },
  {
    find: /^@tupl\/runtime$/,
    replacement: resolve(rootDir, "../../packages/runtime/src/index.ts"),
  },
  {
    find: /^@tupl\/provider-drizzle$/,
    replacement: resolve(rootDir, "../../packages/provider-drizzle/src/index.ts"),
  },
  {
    find: /^@tupl\/provider-ioredis$/,
    replacement: resolve(rootDir, "../../packages/provider-ioredis/src/index.ts"),
  },
  {
    find: /^@tupl\/provider-objection$/,
    replacement: resolve(rootDir, "../../packages/provider-objection/src/index.ts"),
  },
  {
    find: /^@tupl\/provider-kysely$/,
    replacement: resolve(rootDir, "../../packages/provider-kysely/src/index.ts"),
  },
] as const;

export default defineConfig(() => {
  return {
    plugins: [react(), tsconfigPaths()],
    resolve: {
      alias: [{ find: /^@\//, replacement: `${resolve(rootDir, "./src")}/` }, ...workspaceAliases],
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
