import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { join, normalize, resolve } from "node:path";
import { describe, expect, it } from "vitest";

const REPO_ROOT = process.cwd();

const REQUIRED_PATHS = [
  "AGENTS.md",
  "ARCHITECTURE.md",
  "docs/index.md",
  "docs/design-docs/index.md",
  "docs/design-docs/core-beliefs.md",
  "docs/design-docs/relational-pipeline.md",
  "docs/design-docs/provider-model.md",
  "docs/design-docs/planner-invariants.md",
  "docs/exec-plans/active",
  "docs/exec-plans/active/README.md",
  "docs/exec-plans/completed",
  "docs/exec-plans/completed/algebra-pipeline-reset.md",
  "docs/exec-plans/completed/translation-introspection-provider-reset.md",
  "docs/exec-plans/tech-debt-tracker.md",
];

const INDEX_FILES = ["AGENTS.md", "ARCHITECTURE.md", "docs/index.md", "docs/design-docs/index.md"];

const MARKDOWN_LINK_PATTERN = /\[[^\]]+\]\(([^)]+)\)/g;

function isDirectory(relativePath: string) {
  return statSync(join(REPO_ROOT, relativePath)).isDirectory();
}

function listMarkdownFiles(relativePath: string): string[] {
  const root = join(REPO_ROOT, relativePath);
  const out: string[] = [];

  for (const entry of readdirSync(root)) {
    const absolute = join(root, entry);
    const relative = join(relativePath, entry);
    if (statSync(absolute).isDirectory()) {
      out.push(...listMarkdownFiles(relative));
      continue;
    }
    if (relative.endsWith(".md")) {
      out.push(relative);
    }
  }

  return out;
}

function collectLocalMarkdownLinks(relativeFile: string): string[] {
  const contents = readFileSync(join(REPO_ROOT, relativeFile), "utf8");
  const links: string[] = [];

  for (const match of contents.matchAll(MARKDOWN_LINK_PATTERN)) {
    const target = match[1];
    if (!target || target.startsWith("http://") || target.startsWith("https://")) {
      continue;
    }
    if (target.startsWith("#")) {
      continue;
    }
    links.push(target);
  }

  return links;
}

function resolveRepoLocalLink(sourceFile: string, target: string): string {
  const withoutAnchor = target.split("#")[0] ?? target;
  const withoutQuery = withoutAnchor.split("?")[0] ?? withoutAnchor;
  return normalize(resolve(join(REPO_ROOT, sourceFile, ".."), withoutQuery));
}

describe("docs knowledge store", () => {
  it("contains the required knowledge-store files and directories", () => {
    for (const relativePath of REQUIRED_PATHS) {
      expect(existsSync(join(REPO_ROOT, relativePath)), relativePath).toBe(true);
    }

    expect(isDirectory("docs/exec-plans/active")).toBe(true);
    expect(isDirectory("docs/exec-plans/completed")).toBe(true);
  });

  it("keeps docs indexes and AGENTS links repo-local and valid", () => {
    for (const sourceFile of INDEX_FILES) {
      for (const target of collectLocalMarkdownLinks(sourceFile)) {
        const resolved = resolveRepoLocalLink(sourceFile, target);
        expect(
          resolved.startsWith(REPO_ROOT),
          `${sourceFile} should only link to repo-local targets: ${target}`,
        ).toBe(true);
        expect(existsSync(resolved), `${sourceFile} -> ${target}`).toBe(true);
      }
    }
  });

  it("keeps the design-doc and execution-plan structure populated", () => {
    const designDocs = listMarkdownFiles("docs/design-docs");
    const completedPlans = listMarkdownFiles("docs/exec-plans/completed");

    expect(designDocs.length).toBeGreaterThanOrEqual(5);
    expect(completedPlans.length).toBeGreaterThanOrEqual(2);
  });
});
