import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { describe, expect, it } from "vitest";

const REPO_ROOT = process.cwd();
const IMPORT_PATTERN = /(?:from\s+["']([^"']+)["']|import\s*\(\s*["']([^"']+)["']\s*\))/g;

const LAYER_RULES = {
  "packages/foundation/src": new Set<string>(["@tupl/foundation"]),
  "packages/provider-kit/src": new Set<string>(["@tupl/foundation", "@tupl/provider-kit"]),
  "packages/schema-model/src": new Set<string>([
    "@tupl/foundation",
    "@tupl/provider-kit",
    "@tupl/schema-model",
  ]),
  "packages/planner/src": new Set<string>([
    "@tupl/foundation",
    "@tupl/provider-kit",
    "@tupl/schema-model",
    "@tupl/planner",
  ]),
  "packages/runtime/src": new Set<string>([
    "@tupl/foundation",
    "@tupl/provider-kit",
    "@tupl/schema-model",
    "@tupl/planner",
    "@tupl/runtime",
  ]),
  "packages/schema/src": new Set<string>(["@tupl/schema-model", "@tupl/runtime", "@tupl/schema"]),
  "packages/test-support/src": new Set<string>([
    "@tupl/foundation",
    "@tupl/provider-kit",
    "@tupl/schema-model",
    "@tupl/planner",
    "@tupl/runtime",
    "@tupl/test-support",
  ]),
} as const;

const DISALLOWED_PUBLIC_REFS = [
  "README.md",
  "docs",
  "examples",
  "packages/provider-drizzle",
  "packages/provider-ioredis",
  "packages/provider-kysely",
  "packages/provider-objection",
  "packages/schema/README.md",
] as const;

const DIRECT_SUBPATH_EXPORTS = [
  {
    name: "@tupl/provider-kit/shapes",
    subpath: "./shapes",
    target: "packages/provider-kit/src/provider/shapes/index.ts",
    packageJson: "packages/provider-kit/package.json",
  },
  {
    name: "@tupl/provider-kit/testing",
    subpath: "./testing",
    target: "packages/provider-kit/src/testing.ts",
    packageJson: "packages/provider-kit/package.json",
  },
  {
    name: "@tupl/runtime/executor",
    subpath: "./executor",
    target: "packages/runtime/src/runtime/executor.ts",
    packageJson: "packages/runtime/package.json",
  },
] as const;

const DISALLOWED_WRAPPER_TARGETS = [
  "packages/provider-kit/src/shapes/index.ts",
  "packages/runtime/src/executor.ts",
  "packages/runtime/src/runtime/errors.ts",
  "packages/schema-model/src/schema/index.ts",
] as const;

const STRUCTURAL_LINE_BUDGETS = {
  "packages/planner/src/planner/planning.ts": 200,
  "packages/planner/src/planner/sql-lowering.ts": 250,
  "packages/planner/src/planner/query-shape-validation.ts": 1200,
  "packages/planner/src/planner/structured-select-lowering.ts": 1200,
  "packages/planner/src/planner/simple-select-lowering.ts": 200,
  "packages/planner/src/planner/select-shape.ts": 400,
  "packages/planner/src/planner/select-join-tree.ts": 400,
  "packages/planner/src/planner/select-project.ts": 300,
  "packages/planner/src/planner/select-projections.ts": 800,
  "packages/planner/src/planner/select-from-lowering.ts": 400,
  "packages/planner/src/planner/where-lowering.ts": 1200,
  "packages/planner/src/planner/sql-expr-lowering.ts": 1200,
  "packages/planner/src/planner/sql-expr-utils.ts": 200,
  "packages/planner/src/planner/expr-literals.ts": 300,
  "packages/planner/src/planner/expr-column-refs.ts": 300,
  "packages/planner/src/planner/expr-subqueries.ts": 300,
  "packages/planner/src/planner/expr-functions.ts": 300,
  "packages/planner/src/planner/aggregate-lowering.ts": 1200,
  "packages/planner/src/planner/aggregate-ordering.ts": 200,
  "packages/planner/src/planner/group-by-resolution.ts": 300,
  "packages/planner/src/planner/aggregate-order-resolution.ts": 400,
  "packages/planner/src/planner/having-lowering.ts": 800,
  "packages/planner/src/planner/view-expansion.ts": 1200,
  "packages/planner/src/planner/provider-fragments.ts": 1200,
  "packages/planner/src/planner/conventions.ts": 1200,
  "packages/planner/src/planner/physical-planning.ts": 1200,
  "packages/runtime/src/runtime/local-execution.ts": 300,
  "packages/runtime/src/runtime/plan-graph.ts": 50,
  "packages/runtime/src/runtime/execution-plan-builder.ts": 200,
  "packages/runtime/src/runtime/execution-graph.ts": 600,
  "packages/runtime/src/runtime/explain-shaping.ts": 300,
  "packages/runtime/src/runtime/step-families.ts": 300,
  "packages/runtime/src/runtime/session.ts": 100,
  "packages/runtime/src/runtime/provider-fragment-session.ts": 200,
  "packages/runtime/src/runtime/provider-session-lifecycle.ts": 200,
  "packages/runtime/src/runtime/provider-fragment-errors.ts": 200,
  "packages/runtime/src/runtime/provider-fragment-replay.ts": 300,
  "packages/runtime/src/runtime/rel-execution-session.ts": 800,
  "packages/runtime/src/runtime/query-session-factory.ts": 250,
  "packages/runtime/src/runtime/remote-subtree.ts": 800,
  "packages/runtime/src/runtime/scan-execution.ts": 800,
  "packages/runtime/src/runtime/lookup-join.ts": 800,
  "packages/runtime/src/runtime/local-operators.ts": 800,
  "packages/runtime/src/runtime/window-execution.ts": 800,
  "packages/runtime/src/runtime/expression-eval.ts": 800,
  "packages/runtime/src/runtime/subquery-preparation.ts": 800,
  "packages/runtime/src/runtime/row-ops.ts": 800,
  "packages/planner/src/planner/view-lowering.ts": 800,
  "packages/schema-model/src/types.ts": 150,
  "packages/schema-model/src/normalization.ts": 150,
  "packages/schema-model/src/builder.ts": 400,
  "packages/schema-model/src/builder-helpers.ts": 200,
  "packages/schema-model/src/dsl-tokens.ts": 300,
  "packages/schema-model/src/typed-column-builders.ts": 300,
  "packages/schema-model/src/dsl-column-exprs.ts": 300,
  "packages/schema-model/src/dsl-view-helpers.ts": 300,
  "packages/schema-model/src/mapping.ts": 250,
  "packages/schema-model/src/rel-output-inference.ts": 200,
  "packages/schema-model/src/row-coercion.ts": 300,
  "packages/schema-model/src/logical-row-mapping.ts": 200,
  "packages/schema-model/src/output-inference.ts": 500,
  "packages/schema-model/src/rel-output-mapping.ts": 200,
} as const;

function walkFiles(root: string): string[] {
  const entries = readdirSync(root);
  const out: string[] = [];

  for (const entry of entries) {
    const path = join(root, entry);
    const stat = statSync(path);
    if (stat.isDirectory()) {
      out.push(...walkFiles(path));
      continue;
    }
    out.push(path);
  }

  return out;
}

function getWorkspaceImports(contents: string): string[] {
  const imports = new Set<string>();
  for (const match of contents.matchAll(IMPORT_PATTERN)) {
    const specifier = match[1] ?? match[2];
    if (!specifier?.startsWith("@tupl/")) {
      continue;
    }
    imports.add(rootPackageOf(specifier));
  }
  return [...imports];
}

function rootPackageOf(specifier: string): string {
  const [scope, name] = specifier.split("/");
  return `${scope}/${name}`;
}

function isWrapperOnlyFile(contents: string): boolean {
  const body = contents
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/\/\/.*$/gm, "")
    .trim();

  if (body.length === 0) {
    return false;
  }

  const lines = body
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
  return (
    lines.length === 1 && /^export\s+(\*|\{[^}]+\})\s+from\s+["'][^"']+["'];?$/.test(lines[0] ?? "")
  );
}

describe("package boundaries", () => {
  it("keeps the schema facade explicitly curated", () => {
    const schemaIndex = readFileSync(join(REPO_ROOT, "packages/schema/src/index.ts"), "utf8");
    expect(schemaIndex).not.toMatch(/export\s+\*\s+from\s+["']@tupl\/schema-model["']/);
    expect(schemaIndex).not.toMatch(/export\s+\*\s+from\s+["']@tupl\/runtime["']/);
    expect(schemaIndex).not.toContain("QueryExecutionPlan");
    expect(schemaIndex).not.toContain("QueryStepEvent");
    expect(schemaIndex).not.toContain("validateTableConstraintRows");
  });

  it("keeps the semantic package graph acyclic and downward-only", () => {
    for (const [dir, allowedImports] of Object.entries(LAYER_RULES)) {
      for (const file of walkFiles(join(REPO_ROOT, dir))) {
        if (!file.endsWith(".ts") && !file.endsWith(".tsx")) {
          continue;
        }

        const imports = getWorkspaceImports(readFileSync(file, "utf8"));
        const disallowed = imports.filter((pkg) => !allowedImports.has(pkg));
        expect(
          disallowed,
          `${relative(REPO_ROOT, file)} imported disallowed packages: ${disallowed.join(", ")}`,
        ).toEqual([]);
      }
    }
  });

  it("keeps legacy package names out of docs, examples, and provider packages", () => {
    const offenders: string[] = [];

    for (const target of DISALLOWED_PUBLIC_REFS) {
      const fullPath = join(REPO_ROOT, target);
      const files = statSync(fullPath).isDirectory() ? walkFiles(fullPath) : [fullPath];
      for (const file of files) {
        if (file.includes("/dist/")) {
          continue;
        }

        const contents = readFileSync(file, "utf8");
        if (contents.includes("@tupl/core") || contents.includes("@tupl-internal/")) {
          offenders.push(relative(REPO_ROOT, file));
        }
      }
    }

    expect(offenders).toEqual([]);
  });

  it("keeps canonical public subpaths pointing at real modules", () => {
    for (const entry of DIRECT_SUBPATH_EXPORTS) {
      const pkg = JSON.parse(readFileSync(join(REPO_ROOT, entry.packageJson), "utf8")) as {
        exports: Record<string, string>;
      };
      const packageDir = join(REPO_ROOT, entry.packageJson, "..");
      const expectedTarget = `./${relative(packageDir, join(REPO_ROOT, entry.target)).replaceAll("\\", "/")}`;
      expect(pkg.exports[entry.subpath], entry.name).toBe(expectedTarget);
    }
  });

  it("avoids wrapper-only files outside package roots and public subpath roots", () => {
    const offenders: string[] = [];

    for (const pkgDir of readdirSync(join(REPO_ROOT, "packages"))) {
      const srcDir = join(REPO_ROOT, "packages", pkgDir, "src");
      if (!statSync(srcDir).isDirectory()) {
        continue;
      }

      for (const file of walkFiles(srcDir)) {
        if (!file.endsWith(".ts") || file.endsWith(".d.ts")) {
          continue;
        }

        const relFile = relative(REPO_ROOT, file);
        if (relFile.endsWith("/index.ts") && relFile === `packages/${pkgDir}/src/index.ts`) {
          continue;
        }
        if (isWrapperOnlyFile(readFileSync(file, "utf8"))) {
          offenders.push(relFile);
        }
      }
    }

    expect(offenders).toEqual([]);
  });

  it("keeps package-local test support on the owning layer or below", () => {
    for (const [dir, allowedImports] of Object.entries(LAYER_RULES)) {
      const supportRoot = join(REPO_ROOT, dir);
      for (const file of walkFiles(supportRoot)) {
        if (!file.includes("/__tests__/support/") || !file.endsWith(".ts")) {
          continue;
        }

        const imports = getWorkspaceImports(readFileSync(file, "utf8"));
        const disallowed = imports.filter((pkg) => !allowedImports.has(pkg));
        expect(
          disallowed,
          `${relative(REPO_ROOT, file)} imported disallowed packages: ${disallowed.join(", ")}`,
        ).toEqual([]);
      }
    }
  });

  it("keeps low-level packages and tests off the schema facade", () => {
    const offenders: string[] = [];

    for (const file of walkFiles(join(REPO_ROOT, "packages"))) {
      if (!file.endsWith(".ts") && !file.endsWith(".tsx")) {
        continue;
      }

      const relFile = relative(REPO_ROOT, file);
      if (relFile.startsWith("packages/schema/")) {
        continue;
      }

      const contents = readFileSync(file, "utf8");
      if (contents.includes(`from "@tupl/schema"`) || contents.includes(`from '@tupl/schema'`)) {
        offenders.push(relFile);
      }
    }

    for (const file of walkFiles(join(REPO_ROOT, "test"))) {
      if (!file.endsWith(".ts") && !file.endsWith(".tsx")) {
        continue;
      }

      const relFile = relative(REPO_ROOT, file);
      if (relFile === "test/public-package-imports.test.ts") {
        continue;
      }

      const contents = readFileSync(file, "utf8");
      if (contents.includes(`from "@tupl/schema"`) || contents.includes(`from '@tupl/schema'`)) {
        offenders.push(relFile);
      }
    }

    expect(offenders).toEqual([]);
  });

  it("keeps long relative helper traversal out of the repo", () => {
    const offenders: string[] = [];

    for (const file of walkFiles(REPO_ROOT)) {
      if (!file.endsWith(".ts") && !file.endsWith(".tsx")) {
        continue;
      }
      if (relative(REPO_ROOT, file) === "test/package-boundaries.test.ts") {
        continue;
      }

      const contents = readFileSync(file, "utf8");
      if (contents.includes("/test/support/") || contents.includes("/__tests__/support/")) {
        offenders.push(relative(REPO_ROOT, file));
      }
    }

    expect(offenders).toEqual([]);
  });

  it("keeps private test-support imports out of product source", () => {
    const offenders: string[] = [];

    for (const pkgDir of readdirSync(join(REPO_ROOT, "packages"))) {
      const srcDir = join(REPO_ROOT, "packages", pkgDir, "src");
      if (!statSync(srcDir).isDirectory()) {
        continue;
      }

      for (const file of walkFiles(srcDir)) {
        if (!file.endsWith(".ts") || file.includes("/__tests__/")) {
          continue;
        }

        const contents = readFileSync(file, "utf8");
        if (contents.includes("@tupl/test-support")) {
          offenders.push(relative(REPO_ROOT, file));
        }
      }
    }

    expect(offenders).toEqual([]);
  });

  it("keeps foundation free of testing surfaces", () => {
    const offenders: string[] = [];

    for (const file of walkFiles(join(REPO_ROOT, "packages/foundation/src"))) {
      if (!file.endsWith(".ts")) {
        continue;
      }

      const contents = readFileSync(file, "utf8");
      if (
        contents.includes("@tupl/test-support") ||
        contents.includes("@tupl/provider-kit/testing")
      ) {
        offenders.push(relative(REPO_ROOT, file));
      }
    }

    expect(offenders).toEqual([]);
  });

  it("keeps first-party adapter conformance on the public provider-kit/testing surface", () => {
    const contents = readFileSync(join(REPO_ROOT, "test/providers/conformance.test.ts"), "utf8");
    expect(contents).toContain("@tupl/provider-kit/testing");
  });

  it("keeps first-party providers on the provider-kit adapter facade", () => {
    const offenders: string[] = [];
    const providerRoots = [
      "packages/provider-drizzle/src",
      "packages/provider-ioredis/src",
      "packages/provider-kysely/src",
      "packages/provider-objection/src",
    ];

    for (const root of providerRoots) {
      for (const file of walkFiles(join(REPO_ROOT, root))) {
        if (!file.endsWith(".ts") && !file.endsWith(".tsx")) {
          continue;
        }

        const contents = readFileSync(file, "utf8");
        if (contents.includes("@tupl/schema-model")) {
          offenders.push(relative(REPO_ROOT, file));
        }
      }
    }

    expect(offenders).toEqual([]);
  });

  it("removes the temporary internal core monoliths", () => {
    expect(() =>
      statSync(join(REPO_ROOT, "packages/schema-model/src/schema-model-core.ts")),
    ).toThrow();
    expect(() =>
      statSync(join(REPO_ROOT, "packages/runtime/src/runtime/query-runner-core.ts")),
    ).toThrow();
    expect(() =>
      statSync(join(REPO_ROOT, "packages/planner/src/planner/query-runner-core.ts")),
    ).toThrow();
    expect(() =>
      statSync(join(REPO_ROOT, "packages/planner/src/planner/sql-lowering-core.ts")),
    ).toThrow();
  });

  it("keeps runtime free of schema-view lowering logic", () => {
    const offenders: string[] = [];

    for (const file of walkFiles(join(REPO_ROOT, "packages/runtime/src"))) {
      if (!file.endsWith(".ts")) {
        continue;
      }

      const contents = readFileSync(file, "utf8");
      if (
        contents.includes("SchemaViewRelNode") ||
        contents.includes("compileViewRelToExecutableResult") ||
        contents.includes("compileSchemaViewRelNodeResult") ||
        contents.includes("rewriteViewBindingExprForExecution")
      ) {
        offenders.push(relative(REPO_ROOT, file));
      }
    }

    expect(offenders).toEqual([]);
  });

  it("keeps planner-owned and runtime-owned modules within structural budgets", () => {
    const offenders: string[] = [];

    for (const [file, limit] of Object.entries(STRUCTURAL_LINE_BUDGETS)) {
      const lineCount = readFileSync(join(REPO_ROOT, file), "utf8").split("\n").length;
      if (lineCount > limit) {
        offenders.push(`${file} (${lineCount} > ${limit})`);
      }
    }

    expect(offenders).toEqual([]);
  });

  it("keeps workspace tooling off deleted wrapper paths", () => {
    const offenders: string[] = [];
    const files = [
      "tsconfig.json",
      "vitest.config.ts",
      "vitest.fast.config.ts",
      "vitest.playground-slow.config.ts",
      "examples/playground/tsconfig.json",
      "examples/playground/vite.config.ts",
    ];

    for (const file of files) {
      const contents = readFileSync(join(REPO_ROOT, file), "utf8");
      for (const target of DISALLOWED_WRAPPER_TARGETS) {
        if (contents.includes(target)) {
          offenders.push(`${file}: ${target}`);
        }
      }
    }

    expect(offenders).toEqual([]);
  });
});
