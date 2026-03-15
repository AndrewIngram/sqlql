import { useMemo, useState } from "react";
import type React from "react";
import Editor, { type OnMount } from "@monaco-editor/react";
import { ChevronDown, ChevronRight } from "lucide-react";
import { normalizePhysicalPlanForSnapshot, normalizeRelForSnapshot } from "@tupl/planner";
import type { ExplainResult } from "@tupl/schema";

import type { ExecutedProviderOperation } from "./types";
import { Badge } from "./components/ui/badge";
import { Button } from "./components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "./components/ui/collapsible";
import { ScrollArea } from "./components/ui/scroll-area";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "./components/ui/table";
import { cn } from "./lib/utils";

const MONACO_INDENT_OPTIONS = {
  detectIndentation: false,
  insertSpaces: true,
  tabSize: 2,
} as const;

function executedOperationSqlModelPath(index: number): string {
  return `inmemory://tupl/executed-operation-${index}.sql`;
}

export function JsonBlock({ value }: { value: unknown }): React.JSX.Element {
  return (
    <ScrollArea className="h-40 rounded-md border bg-slate-50 p-2">
      <pre className="font-mono text-xs text-slate-700">{JSON.stringify(value, null, 2)}</pre>
    </ScrollArea>
  );
}

export function ExecutedProviderOperationsPanel({
  operations,
  onMonacoMount,
  className,
}: {
  operations: ExecutedProviderOperation[];
  onMonacoMount: OnMount;
  className?: string;
}): React.JSX.Element {
  return (
    <div
      className={cn(
        "flex h-full min-h-0 flex-col overflow-hidden rounded-md border bg-white",
        className,
      )}
    >
      <div className="border-b bg-slate-50 px-3 py-2">
        <div className="text-sm font-semibold text-slate-900">
          Executed provider operations ({operations.length})
        </div>
      </div>
      {operations.length === 0 ? (
        <div className="px-3 py-4 text-xs text-slate-500">
          No provider operations executed for this run.
        </div>
      ) : (
        <ScrollArea className="min-h-0 flex-1 px-3 py-2">
          <div className="space-y-3">
            {operations.map((entry, index) => {
              const isSql = entry.kind === "sql_query";
              const editorHeight = isSql
                ? Math.max(72, Math.min(220, (entry.sql.split("\n").length + 1) * 20))
                : 120;
              return (
                <div key={`executed-query-${index}`} className="overflow-hidden rounded-md border">
                  <div className="border-b bg-slate-50 px-2 py-1">
                    <div className="flex items-center gap-2 text-[11px] text-slate-600">
                      <span>Operation {index + 1}</span>
                      <Badge
                        variant="secondary"
                        className="h-5 rounded-sm px-1.5 text-[10px] font-medium"
                      >
                        {entry.provider}
                      </Badge>
                      <Badge
                        variant="outline"
                        className="h-5 rounded-sm px-1.5 text-[10px] font-medium"
                      >
                        {isSql ? "SQL query" : "Redis lookup"}
                      </Badge>
                    </div>
                  </div>
                  {isSql ? (
                    <Editor
                      path={executedOperationSqlModelPath(index)}
                      language="sql"
                      value={entry.sql}
                      onMount={onMonacoMount}
                      options={{
                        minimap: { enabled: false },
                        fontSize: 12,
                        readOnly: true,
                        scrollBeyondLastLine: false,
                        lineNumbers: "off",
                        wordWrap: "on",
                        ...MONACO_INDENT_OPTIONS,
                      }}
                      height={`${editorHeight}px`}
                    />
                  ) : (
                    <div className="px-2 py-2">
                      <JsonBlock value={entry.lookup} />
                    </div>
                  )}
                  <div className="border-t bg-slate-50 px-2 py-1 font-mono text-[11px] text-slate-600">
                    variables: {JSON.stringify(entry.variables)}
                  </div>
                </div>
              );
            })}
          </div>
        </ScrollArea>
      )}
    </div>
  );
}

export function renderRows(
  rows: Array<Record<string, unknown>>,
  options?: { heightClassName?: string; frameClassName?: string; expandNestedObjects?: boolean },
): React.JSX.Element {
  if (rows.length === 0) {
    return <div className="text-sm text-slate-500">No rows.</div>;
  }

  const normalizedRows = options?.expandNestedObjects
    ? rows.map((row) => {
        const out: Record<string, unknown> = {};
        for (const [key, value] of Object.entries(row)) {
          if (value && typeof value === "object" && !Array.isArray(value)) {
            const nestedEntries = Object.entries(value as Record<string, unknown>);
            if (nestedEntries.length === 0) {
              out[key] = null;
              continue;
            }
            for (const [nestedKey, nestedValue] of nestedEntries) {
              out[`${key}.${nestedKey}`] = nestedValue;
            }
            continue;
          }

          out[key] = value;
        }
        return out;
      })
    : rows;

  const columns = [...new Set(normalizedRows.flatMap((row) => Object.keys(row)))];

  return (
    <ScrollArea
      className={cn(
        options?.heightClassName ?? "h-[460px]",
        options?.frameClassName ?? "rounded-md border bg-white",
      )}
    >
      <Table>
        <TableHeader>
          <TableRow>
            {columns.map((column) => (
              <TableHead key={column} className="sticky top-0 bg-slate-100/95">
                {column}
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {normalizedRows.map((row, rowIndex) => (
            <TableRow key={`row-${rowIndex}`}>
              {columns.map((column) => (
                <TableCell key={`${rowIndex}:${column}`} className="font-mono text-xs">
                  {JSON.stringify(row[column] ?? null)}
                </TableCell>
              ))}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </ScrollArea>
  );
}

export function StepSection({
  title,
  defaultOpen = true,
  children,
}: {
  title: string;
  defaultOpen?: boolean;
  children: React.ReactNode;
}): React.JSX.Element {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <Collapsible open={open} onOpenChange={setOpen} className="rounded-md border bg-slate-50 p-3">
      <CollapsibleTrigger asChild>
        <Button
          variant="ghost"
          size="sm"
          className="h-auto w-full justify-between px-0 py-0 text-sm font-semibold"
        >
          {title}
          {open ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
        </Button>
      </CollapsibleTrigger>
      <CollapsibleContent className="mt-3 space-y-2">{children}</CollapsibleContent>
    </Collapsible>
  );
}

function normalizeExplainFragments(explain: ExplainResult) {
  return explain.fragments.map((fragment) => ({
    id: fragment.id,
    convention: fragment.convention,
    ...(fragment.provider ? { provider: fragment.provider } : {}),
    rel: normalizeRelForSnapshot(fragment.rel),
  }));
}

function normalizeExplainProviderPlans(explain: ExplainResult) {
  return explain.providerPlans.map((providerPlan) => ({
    fragmentId: providerPlan.fragmentId,
    provider: providerPlan.provider,
    kind: providerPlan.kind,
    rel: normalizeRelForSnapshot(providerPlan.rel),
    ...(providerPlan.description
      ? { description: providerPlan.description }
      : { descriptionUnavailable: true }),
  }));
}

export function TranslationExplainPanel({
  explain,
}: {
  explain: ExplainResult | null;
}): React.JSX.Element {
  const normalizedInitialRel = useMemo(
    () => (explain ? normalizeRelForSnapshot(explain.initialRel) : null),
    [explain],
  );
  const normalizedRewrittenRel = useMemo(
    () => (explain ? normalizeRelForSnapshot(explain.rewrittenRel) : null),
    [explain],
  );
  const normalizedPhysicalPlan = useMemo(
    () => (explain ? normalizePhysicalPlanForSnapshot(explain.physicalPlan) : null),
    [explain],
  );
  const normalizedFragments = useMemo(
    () => (explain ? normalizeExplainFragments(explain) : []),
    [explain],
  );
  const normalizedProviderPlans = useMemo(
    () => (explain ? normalizeExplainProviderPlans(explain) : []),
    [explain],
  );

  if (!explain) {
    return (
      <div className="flex h-full items-center justify-center rounded-none border-r bg-slate-50 px-4 text-sm text-slate-500">
        No translation artifacts available yet.
      </div>
    );
  }

  return (
    <ScrollArea className="h-full border-r bg-slate-50">
      <div className="space-y-3 p-3">
        <div className="rounded-md border bg-white px-3 py-2 text-xs text-slate-600">
          Planner nodes:{" "}
          <span className="font-medium text-slate-900">{explain.plannerNodeCount}</span>
        </div>

        <StepSection title="SQL" defaultOpen>
          <pre className="overflow-x-auto whitespace-pre-wrap rounded-md border bg-white p-3 font-mono text-xs text-slate-700">
            {explain.sql}
          </pre>
        </StepSection>

        <StepSection title="Initial Rel" defaultOpen={false}>
          <JsonBlock value={normalizedInitialRel} />
        </StepSection>

        <StepSection title="Rewritten Rel" defaultOpen={false}>
          <JsonBlock value={normalizedRewrittenRel} />
        </StepSection>

        <StepSection title="Physical Fragments" defaultOpen={false}>
          <JsonBlock
            value={{
              physicalPlan: normalizedPhysicalPlan,
              fragments: normalizedFragments,
            }}
          />
        </StepSection>

        <StepSection title="Provider Plans" defaultOpen={false}>
          <JsonBlock value={normalizedProviderPlans} />
        </StepSection>

        {explain.diagnostics.length > 0 ? (
          <StepSection title="Diagnostics" defaultOpen={false}>
            <JsonBlock value={explain.diagnostics} />
          </StepSection>
        ) : null}
      </div>
    </ScrollArea>
  );
}
