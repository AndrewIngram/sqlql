import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type React from "react";
import Editor, { type OnMount } from "@monaco-editor/react";
import { ChevronDown, ChevronRight, Database, SearchCode, Table2, X } from "lucide-react";
import type * as Monaco from "monaco-editor";
import type {
  QueryExecutionPlanScope,
  QueryExecutionPlanStep,
  QueryRow,
  QuerySession,
  QueryStepEvent,
  QueryStepState,
  SchemaDefinition,
} from "sqlql";
import { toSqlDDL } from "sqlql";

import { DataGrid } from "@/data-grid";
import { DataTableJsonEditor } from "@/data-table-json";
import { addEmptyRow, mergeTableRows } from "@/data-editing";
import { buildQueryCatalog, EXAMPLE_PACKS, serializeJson } from "@/examples";
import { PlanGraph } from "@/PlanGraph";
import { buildQueryCompatibilityMap } from "@/query-compatibility";
import { truncateReason } from "@/query-preview";
import {
  canSelectCatalogQuery,
  CUSTOM_QUERY_ID,
  selectionAfterManualSqlEdit,
  selectionAfterSchemaChange,
} from "@/query-selection-state";
import { SchemaRelationsGraph } from "@/SchemaRelationsGraph";
import {
  compilePlaygroundInput,
  createSession,
  runSessionToCompletion,
} from "@/session-runtime";
import { SqlPreviewLine } from "@/SqlPreviewLine";
import { registerSqlCompletionProvider } from "@/sql-completion";
import {
  PLAYGROUND_SCHEMA_JSON_SCHEMA,
  buildTableRowsJsonSchema,
  parseRowsText,
  parseSchemaText,
} from "@/validation";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectSeparator,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { cn } from "@/lib/utils";

const SCHEMA_MODEL_PATH = "inmemory://sqlql/schema.json";
const SCHEMA_DDL_MODEL_PATH = "inmemory://sqlql/schema.ddl.sql";
const SQL_MODEL_PATH = "inmemory://sqlql/query.sql";
const CUSTOM_PRESET_ID = "__custom__";
const EXPANDED_QUERY_EDITOR_PADDING_Y_PX = 16;
const EXPANDED_QUERY_EDITOR_DEFAULT_HEIGHT_PX = 120;
const SCHEMA_SPLIT_MIN_PERCENT = 30;
const SCHEMA_SPLIT_MAX_PERCENT = 70;

type TopTab = "schema" | "data" | "query";
type SchemaTab = "diagram" | "ddl";
type QueryTab = "result" | "explain";
type DataEditorMode = "json" | "grid";

function dataTableModelPath(tableName: string): string {
  return `inmemory://sqlql/data-table-${tableName}.json`;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}

function positionFromIndex(sql: string, rawIndex: number): { line: number; column: number } {
  const index = Math.max(0, Math.min(rawIndex, sql.length));
  let line = 1;
  let column = 1;

  for (let i = 0; i < index; i += 1) {
    if (sql[i] === "\n") {
      line += 1;
      column = 1;
    } else {
      column += 1;
    }
  }

  return { line, column };
}

function rangeFromIndex(sql: string, startIndex: number, endIndexExclusive: number) {
  const start = positionFromIndex(sql, startIndex);
  const end = positionFromIndex(sql, Math.max(startIndex + 1, endIndexExclusive));
  return {
    startLineNumber: start.line,
    startColumn: start.column,
    endLineNumber: end.line,
    endColumn: end.column,
  };
}

function findTokenRangeAtPosition(sql: string, position: number) {
  const tokenChar = /[A-Za-z0-9_.$"]/u;
  const clamped = Math.max(0, Math.min(position, Math.max(0, sql.length - 1)));
  let start = clamped;
  let end = clamped + 1;

  while (start > 0 && tokenChar.test(sql[start - 1] ?? "")) {
    start -= 1;
  }
  while (end < sql.length && tokenChar.test(sql[end] ?? "")) {
    end += 1;
  }

  if (start === end) {
    end = Math.min(sql.length, start + 1);
  }
  return rangeFromIndex(sql, start, end);
}

function findIdentifierRange(sql: string, identifier: string) {
  if (!identifier) {
    return undefined;
  }

  const pattern = new RegExp(`\\b${escapeRegExp(identifier)}\\b`, "iu");
  const match = pattern.exec(sql);
  if (!match || match.index == null) {
    return undefined;
  }

  return rangeFromIndex(sql, match.index, match.index + match[0].length);
}

function findQualifiedIdentifierRange(sql: string, qualifier: string, identifier: string) {
  if (!qualifier || !identifier) {
    return undefined;
  }

  const pattern = new RegExp(
    `\\b${escapeRegExp(qualifier)}\\b\\s*\\.\\s*\\b${escapeRegExp(identifier)}\\b`,
    "iu",
  );
  const match = pattern.exec(sql);
  if (!match || match.index == null) {
    return undefined;
  }

  return rangeFromIndex(sql, match.index, match.index + match[0].length);
}

function findClauseBounds(sql: string, clause: "where" | "order_by"): {
  start: number;
  end: number;
} | null {
  const source = sql;
  const startMatch =
    clause === "where"
      ? /\bwhere\b/iu.exec(source)
      : /\border\s+by\b/iu.exec(source);
  if (!startMatch || startMatch.index == null) {
    return null;
  }

  const start = startMatch.index;
  const remainder = source.slice(start + startMatch[0].length);
  const endPatterns = clause === "where"
    ? [/\bgroup\s+by\b/iu, /\border\s+by\b/iu, /\blimit\b/iu, /\boffset\b/iu, /\bhaving\b/iu, /\bunion\b/iu, /\bintersect\b/iu, /\bexcept\b/iu, /;/u]
    : [/\blimit\b/iu, /\boffset\b/iu, /\bunion\b/iu, /\bintersect\b/iu, /\bexcept\b/iu, /;/u];
  let minRelativeEnd = remainder.length;

  for (const pattern of endPatterns) {
    const match = pattern.exec(remainder);
    if (match?.index == null) {
      continue;
    }
    minRelativeEnd = Math.min(minRelativeEnd, match.index);
  }

  return {
    start,
    end: start + startMatch[0].length + minRelativeEnd,
  };
}

function findQualifiedIdentifierRangeInSlice(
  sql: string,
  qualifier: string,
  identifier: string,
  start: number,
  end: number,
) {
  const slice = sql.slice(start, end);
  const pattern = new RegExp(
    `\\b${escapeRegExp(qualifier)}\\b\\s*\\.\\s*\\b${escapeRegExp(identifier)}\\b`,
    "iu",
  );
  const match = pattern.exec(slice);
  if (!match || match.index == null) {
    return undefined;
  }
  const absoluteStart = start + match.index;
  return rangeFromIndex(sql, absoluteStart, absoluteStart + match[0].length);
}

function findIdentifierRangeInSlice(
  sql: string,
  identifier: string,
  start: number,
  end: number,
) {
  const slice = sql.slice(start, end);
  const pattern = new RegExp(`\\b${escapeRegExp(identifier)}\\b`, "iu");
  const match = pattern.exec(slice);
  if (!match || match.index == null) {
    return undefined;
  }
  const absoluteStart = start + match.index;
  return rangeFromIndex(sql, absoluteStart, absoluteStart + match[0].length);
}

function readAliasesForSql(sqlText: string): Map<string, string> {
  const aliases = new Map<string, string>();
  const aliasRegex = /\b(?:from|join)\s+([a-z_][\w]*)\s*(?:as\s+)?([a-z_][\w]*)?/gi;

  let match = aliasRegex.exec(sqlText);
  while (match) {
    const tableName = match[1];
    if (tableName) {
      const alias = (match[2] ?? tableName).toLowerCase();
      aliases.set(alias, tableName);
    }
    match = aliasRegex.exec(sqlText);
  }

  return aliases;
}

function findStringLiteralRange(sql: string, value: string): ReturnType<typeof rangeFromIndex> | undefined {
  const escapedValue = value.replaceAll("'", "''");
  const pattern = new RegExp(`'${escapeRegExp(escapedValue)}'`, "gu");
  const match = pattern.exec(sql);
  if (!match || match.index == null) {
    return undefined;
  }
  return rangeFromIndex(sql, match.index, match.index + match[0].length);
}

function findEnumLiteralRange(
  sql: string,
  tableName: string,
  columnName: string,
  enumValue: string,
): ReturnType<typeof rangeFromIndex> | undefined {
  const aliases = readAliasesForSql(sql);
  const qualifiers = new Set<string>([tableName.toLowerCase()]);
  for (const [alias, resolvedTable] of aliases.entries()) {
    if (resolvedTable.toLowerCase() === tableName.toLowerCase()) {
      qualifiers.add(alias);
    }
  }

  const escapedValue = enumValue.replaceAll("'", "''");
  const literalPattern = new RegExp(`'${escapeRegExp(escapedValue)}'`, "gu");
  let literalMatch = literalPattern.exec(sql);
  while (literalMatch) {
    if (literalMatch.index == null) {
      literalMatch = literalPattern.exec(sql);
      continue;
    }

    const contextStart = Math.max(0, literalMatch.index - 180);
    const contextText = sql.slice(contextStart, literalMatch.index);
    const simpleColumnPattern = new RegExp(`\\b${escapeRegExp(columnName)}\\b`, "iu");
    const qualifiedPatterns = [...qualifiers].map((qualifier) =>
      new RegExp(
        `\\b${escapeRegExp(qualifier)}\\b\\s*\\.\\s*\\b${escapeRegExp(columnName)}\\b`,
        "iu",
      ));
    const hasColumnContext =
      simpleColumnPattern.test(contextText) ||
      qualifiedPatterns.some((pattern) => pattern.test(contextText));

    if (hasColumnContext) {
      return rangeFromIndex(sql, literalMatch.index, literalMatch.index + literalMatch[0].length);
    }

    literalMatch = literalPattern.exec(sql);
  }

  return findStringLiteralRange(sql, enumValue);
}

function inferSqlErrorRange(sql: string, message: string) {
  const unsupportedFilterMatch =
    /^Filtering on\s+([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s+is not supported by schema policy\.?$/u.exec(
      message,
    );
  if (unsupportedFilterMatch?.[1] && unsupportedFilterMatch?.[2]) {
    const tableName = unsupportedFilterMatch[1];
    const columnName = unsupportedFilterMatch[2];
    const aliases = readAliasesForSql(sql);
    const whereBounds = findClauseBounds(sql, "where");
    const qualifiers = new Set<string>([tableName]);
    for (const [alias, resolvedTable] of aliases.entries()) {
      if (resolvedTable.toLowerCase() === tableName.toLowerCase()) {
        qualifiers.add(alias);
      }
    }

    if (whereBounds) {
      for (const qualifier of qualifiers) {
        const range = findQualifiedIdentifierRangeInSlice(
          sql,
          qualifier,
          columnName,
          whereBounds.start,
          whereBounds.end,
        );
        if (range) {
          return range;
        }
      }
      const range = findIdentifierRangeInSlice(
        sql,
        columnName,
        whereBounds.start,
        whereBounds.end,
      );
      if (range) {
        return range;
      }
    }

    for (const qualifier of qualifiers) {
      const range = findQualifiedIdentifierRange(sql, qualifier, columnName);
      if (range) {
        return range;
      }
    }
    return findIdentifierRange(sql, columnName);
  }

  const unsupportedSortMatch =
    /^Sorting by\s+([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s+is not supported by schema policy\.?$/u.exec(
      message,
    );
  if (unsupportedSortMatch?.[1] && unsupportedSortMatch?.[2]) {
    const tableName = unsupportedSortMatch[1];
    const columnName = unsupportedSortMatch[2];
    const aliases = readAliasesForSql(sql);
    const orderByBounds = findClauseBounds(sql, "order_by");
    const qualifiers = new Set<string>([tableName]);
    for (const [alias, resolvedTable] of aliases.entries()) {
      if (resolvedTable.toLowerCase() === tableName.toLowerCase()) {
        qualifiers.add(alias);
      }
    }

    if (orderByBounds) {
      for (const qualifier of qualifiers) {
        const range = findQualifiedIdentifierRangeInSlice(
          sql,
          qualifier,
          columnName,
          orderByBounds.start,
          orderByBounds.end,
        );
        if (range) {
          return range;
        }
      }
      const range = findIdentifierRangeInSlice(
        sql,
        columnName,
        orderByBounds.start,
        orderByBounds.end,
      );
      if (range) {
        return range;
      }
    }

    for (const qualifier of qualifiers) {
      const range = findQualifiedIdentifierRange(sql, qualifier, columnName);
      if (range) {
        return range;
      }
    }
    return findIdentifierRange(sql, columnName);
  }

  const invalidEnumMatch =
    /^Invalid enum value for\s+([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*):\s*(.+?)\.\s+Allowed values:/u.exec(
      message,
    );
  if (invalidEnumMatch?.[1] && invalidEnumMatch?.[2] && invalidEnumMatch?.[3]) {
    try {
      const parsedValue = JSON.parse(invalidEnumMatch[3]);
      if (typeof parsedValue === "string") {
        const literalRange = findEnumLiteralRange(
          sql,
          invalidEnumMatch[1],
          invalidEnumMatch[2],
          parsedValue,
        );
        if (literalRange) {
          return literalRange;
        }
      }
    } catch {
      // fall through to generic handling
    }

    return findQualifiedIdentifierRange(sql, invalidEnumMatch[1], invalidEnumMatch[2]) ??
      findIdentifierRange(sql, invalidEnumMatch[2]);
  }

  const parserPositionMatch = /\(at position (\d+)\)\s*$/u.exec(message);
  if (parserPositionMatch?.[1]) {
    return findTokenRangeAtPosition(sql, Number.parseInt(parserPositionMatch[1], 10));
  }

  const unknownQualifiedColumnMatch = /^Unknown column:\s*([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)/iu.exec(
    message,
  );
  if (unknownQualifiedColumnMatch?.[1] && unknownQualifiedColumnMatch?.[2]) {
    return findQualifiedIdentifierRange(
      sql,
      unknownQualifiedColumnMatch[1],
      unknownQualifiedColumnMatch[2],
    );
  }

  const unknownColumnMatch = /^Unknown column:\s*([A-Za-z_][A-Za-z0-9_]*)/iu.exec(message);
  if (unknownColumnMatch?.[1]) {
    return findIdentifierRange(sql, unknownColumnMatch[1]);
  }

  const unknownAliasMatch = /^Unknown table alias:\s*([A-Za-z_][A-Za-z0-9_]*)/iu.exec(message);
  if (unknownAliasMatch?.[1]) {
    return findIdentifierRange(sql, unknownAliasMatch[1]);
  }

  const unknownTableMatch =
    /^Unknown table:\s*([A-Za-z_][A-Za-z0-9_]*)/iu.exec(message) ??
    /^No table methods registered for table:\s*([A-Za-z_][A-Za-z0-9_]*)/iu.exec(message) ??
    /^Table not found in schema:\s*([A-Za-z_][A-Za-z0-9_]*)/iu.exec(message);
  if (unknownTableMatch?.[1]) {
    return findIdentifierRange(sql, unknownTableMatch[1]);
  }

  const ambiguousColumnMatch =
    /^Ambiguous unqualified column reference:\s*([A-Za-z_][A-Za-z0-9_]*)/iu.exec(message);
  if (ambiguousColumnMatch?.[1]) {
    return findIdentifierRange(sql, ambiguousColumnMatch[1]);
  }

  return undefined;
}

function findTableLineNumber(schemaText: string, tableName: string): number | null {
  const regex = new RegExp(`^\\s*"${escapeRegExp(tableName)}"\\s*:`, "mu");
  const match = regex.exec(schemaText);
  const index = match?.index ?? schemaText.indexOf(`"${tableName}"`);

  if (index < 0) {
    return null;
  }

  return schemaText.slice(0, index).split("\n").length;
}

function extractRowsForEditing(
  schema: SchemaDefinition | undefined,
  rowsText: string,
  parsedRows: Record<string, QueryRow[]> | undefined,
): Record<string, QueryRow[]> {
  if (!schema) {
    return {};
  }

  if (parsedRows) {
    return parsedRows;
  }

  const fallback = Object.fromEntries(
    Object.keys(schema.tables).map((tableName) => [tableName, [] as QueryRow[]]),
  );

  try {
    const parsed = JSON.parse(rowsText) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return fallback;
    }

    for (const tableName of Object.keys(schema.tables)) {
      const tableRows = (parsed as Record<string, unknown>)[tableName];
      if (!Array.isArray(tableRows)) {
        continue;
      }

      const onlyObjects = tableRows.filter(
        (entry) => entry != null && typeof entry === "object" && !Array.isArray(entry),
      );

      fallback[tableName] = onlyObjects as QueryRow[];
    }

    return fallback;
  } catch {
    return fallback;
  }
}

function tableIssueLines(issues: Array<{ path: string; message: string }>, tableName: string): string[] {
  const prefix = `${tableName}`;
  return issues
    .filter((issue) => issue.path === "$" || issue.path.startsWith(prefix))
    .map((issue) => `${issue.path}: ${issue.message}`);
}

function JsonBlock({ value }: { value: unknown }): React.JSX.Element {
  return (
    <ScrollArea className="h-40 rounded-md border bg-slate-50 p-2">
      <pre className="font-mono text-xs text-slate-700">{JSON.stringify(value, null, 2)}</pre>
    </ScrollArea>
  );
}

function renderRows(
  rows: Array<Record<string, unknown>>,
  options?: { heightClassName?: string; expandNestedObjects?: boolean },
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
    <ScrollArea className={cn(options?.heightClassName ?? "h-[460px]", "rounded-md border bg-white")}>
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

function StepSection({
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
        <Button variant="ghost" size="sm" className="h-auto w-full justify-between px-0 py-0 text-sm font-semibold">
          {title}
          {open ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
        </Button>
      </CollapsibleTrigger>
      <CollapsibleContent className="mt-3 space-y-2">{children}</CollapsibleContent>
    </Collapsible>
  );
}

export function App(): React.JSX.Element {
  const defaultPack = EXAMPLE_PACKS[0];
  const defaultCatalogId = defaultPack?.queries[0] ? `${defaultPack.id}:0` : CUSTOM_QUERY_ID;

  const [activePackId, setActivePackId] = useState(defaultPack?.id ?? CUSTOM_PRESET_ID);
  const [activeTopTab, setActiveTopTab] = useState<TopTab>("schema");
  const [activeSchemaTab, setActiveSchemaTab] = useState<SchemaTab>("diagram");
  const [activeQueryTab, setActiveQueryTab] = useState<QueryTab>("result");

  const [schemaJsonText, setSchemaJsonText] = useState(
    defaultPack ? serializeJson(defaultPack.schema) : '{\n  "tables": {}\n}\n',
  );
  const [rowsJsonText, setRowsJsonText] = useState(
    defaultPack ? serializeJson(defaultPack.rows) : "{}\n",
  );
  const [sqlText, setSqlText] = useState(defaultPack?.queries[0]?.sql ?? "SELECT 1");

  const [selectedSchemaTable, setSelectedSchemaTable] = useState<string | null>(
    defaultPack ? Object.keys(defaultPack.schema.tables)[0] ?? null : null,
  );
  const [selectedDataTable, setSelectedDataTable] = useState<string | null>(
    defaultPack ? Object.keys(defaultPack.schema.tables)[0] ?? null : null,
  );
  const [dataEditorMode, setDataEditorMode] = useState<DataEditorMode>("json");

  const [runtimeError, setRuntimeError] = useState<string | null>(null);
  const [planSteps, setPlanSteps] = useState<QueryExecutionPlanStep[]>([]);
  const [planScopes, setPlanScopes] = useState<QueryExecutionPlanScope[]>([]);
  const [events, setEvents] = useState<QueryStepEvent[]>([]);
  const [resultRows, setResultRows] = useState<Array<Record<string, unknown>> | null>(null);
  const [selectedStepId, setSelectedStepId] = useState<string | null>(null);
  const [selectedCatalogQueryId, setSelectedCatalogQueryId] = useState(defaultCatalogId);
  const [isQueryEditorExpanded, setIsQueryEditorExpanded] = useState(false);
  const [expandedQueryEditorHeightPx, setExpandedQueryEditorHeightPx] = useState(
    EXPANDED_QUERY_EDITOR_DEFAULT_HEIGHT_PX,
  );
  const [schemaSplitPercent, setSchemaSplitPercent] = useState(50);
  const [isSchemaSplitDragging, setIsSchemaSplitDragging] = useState(false);
  const [sessionTick, setSessionTick] = useState(0);

  const monacoRef = useRef<typeof Monaco | null>(null);
  const schemaEditorRef = useRef<Monaco.editor.IStandaloneCodeEditor | null>(null);
  const sqlEditorRef = useRef<Monaco.editor.IStandaloneCodeEditor | null>(null);
  const sqlProviderDisposableRef = useRef<Monaco.IDisposable | null>(null);
  const schemaDecorationIdsRef = useRef<string[]>([]);
  const queryEditorShellRef = useRef<HTMLDivElement | null>(null);
  const schemaWorkspaceRef = useRef<HTMLDivElement | null>(null);

  const sessionRef = useRef<QuerySession | null>(null);
  const executionRequestIdRef = useRef(0);
  const schemaForCompletionRef = useRef<SchemaDefinition | null>(null);
  const clampSchemaSplitPercent = useCallback((value: number): number => {
    return Math.min(SCHEMA_SPLIT_MAX_PERCENT, Math.max(SCHEMA_SPLIT_MIN_PERCENT, value));
  }, []);
  const schemaSplitGridTemplate = useMemo(() => {
    const right = 100 - schemaSplitPercent;
    return `calc(${schemaSplitPercent}% - 5px) 10px calc(${right}% - 5px)`;
  }, [schemaSplitPercent]);
  const recalculateExpandedQueryEditorHeight = useCallback(() => {
    if (!isQueryEditorExpanded) {
      return;
    }

    const editor = sqlEditorRef.current;
    if (!editor) {
      return;
    }

    try {
      const viewportMaxHeight = Math.max(
        1,
        Math.floor(window.innerHeight * 0.5) - EXPANDED_QUERY_EDITOR_PADDING_Y_PX,
      );
      const contentHeight = Math.max(1, Math.ceil(editor.getContentHeight()));
      const nextHeight = Math.min(contentHeight, viewportMaxHeight);
      setExpandedQueryEditorHeightPx(nextHeight);
    } catch {
      setExpandedQueryEditorHeightPx(EXPANDED_QUERY_EDITOR_DEFAULT_HEIGHT_PX);
    }
  }, [isQueryEditorExpanded]);
  const startSchemaSplitDrag = useCallback((event: React.PointerEvent<HTMLButtonElement>) => {
    event.preventDefault();
    setIsSchemaSplitDragging(true);
  }, []);

  const activePack = useMemo(
    () => EXAMPLE_PACKS.find((pack) => pack.id === activePackId),
    [activePackId],
  );
  const queryCatalog = useMemo(() => buildQueryCatalog(EXAMPLE_PACKS), []);

  const schemaParse = useMemo(() => parseSchemaText(schemaJsonText), [schemaJsonText]);
  const queryCompatibilityById = useMemo(
    () => buildQueryCompatibilityMap(schemaParse, queryCatalog),
    [queryCatalog, schemaParse],
  );
  const rowsParse = useMemo(() => {
    if (!schemaParse.ok || !schemaParse.schema) {
      return {
        ok: false,
        issues: [{ path: "$", message: "Fix schema JSON first." }],
      };
    }

    return parseRowsText(schemaParse.schema, rowsJsonText);
  }, [rowsJsonText, schemaParse]);

  const ddlText = useMemo(() => {
    if (!schemaParse.ok || !schemaParse.schema) {
      return "";
    }

    try {
      return toSqlDDL(schemaParse.schema, { ifNotExists: true });
    } catch {
      return "";
    }
  }, [schemaParse]);

  const schemaTableNames = useMemo(
    () => (schemaParse.ok && schemaParse.schema ? Object.keys(schemaParse.schema.tables) : []),
    [schemaParse],
  );

  const editableRowsByTable = useMemo(
    () =>
      extractRowsForEditing(
        schemaParse.ok ? schemaParse.schema : undefined,
        rowsJsonText,
        rowsParse.ok ? rowsParse.rows : undefined,
      ),
    [rowsJsonText, rowsParse, schemaParse],
  );

  const currentDataTable = selectedDataTable && schemaTableNames.includes(selectedDataTable)
    ? selectedDataTable
    : schemaTableNames[0] ?? null;

  const currentDataTableDefinition =
    currentDataTable && schemaParse.ok && schemaParse.schema
      ? schemaParse.schema.tables[currentDataTable]
      : undefined;

  const currentDataRows = currentDataTable ? editableRowsByTable[currentDataTable] ?? [] : [];
  const currentDataMode: DataEditorMode = dataEditorMode;

  const currentTableIssues =
    !rowsParse.ok && currentDataTable
      ? tableIssueLines(rowsParse.issues, currentDataTable)
      : [];

  const currentStepId = events.length > 0 ? (events[events.length - 1]?.id ?? null) : null;

  const statesById = useMemo(() => {
    const map: Record<string, QueryStepState | undefined> = {};
    const session = sessionRef.current;
    if (!session) {
      return map;
    }

    for (const step of planSteps) {
      map[step.id] = session.getStepState(step.id);
    }

    return map;
  }, [planSteps, sessionTick]);

  const selectedStep = selectedStepId
    ? (planSteps.find((step) => step.id === selectedStepId) ?? null)
    : null;
  const selectedStepState = selectedStep ? statesById[selectedStep.id] : undefined;
  const queryCatalogByPack = useMemo(() => {
    return EXAMPLE_PACKS.map((pack) => ({
      packId: pack.id,
      packLabel: pack.label,
      entries: queryCatalog.filter((entry) => entry.packId === pack.id),
    })).filter((group) => group.entries.length > 0);
  }, [queryCatalog]);

  const applyJsonSchemas = useCallback((): void => {
    schemaForCompletionRef.current = schemaParse.ok ? schemaParse.schema ?? null : null;
    const monaco = monacoRef.current;
    if (!monaco) {
      return;
    }

    const schemas: Array<{ uri: string; fileMatch: string[]; schema: Record<string, unknown> }> = [
      {
        uri: "sqlql://schema-format",
        fileMatch: [SCHEMA_MODEL_PATH],
        schema: PLAYGROUND_SCHEMA_JSON_SCHEMA,
      },
    ];
    if (currentDataTable && currentDataTableDefinition) {
      schemas.push({
        uri: `sqlql://rows-table-${currentDataTable}`,
        fileMatch: [dataTableModelPath(currentDataTable)],
        schema: buildTableRowsJsonSchema(currentDataTableDefinition),
      });
    }

    monaco.languages.json.jsonDefaults.setDiagnosticsOptions({
      validate: true,
      allowComments: false,
      schemas,
    });
  }, [currentDataTable, currentDataTableDefinition, schemaParse]);

  const applySqlMarkers = useCallback((): void => {
    const monaco = monacoRef.current;
    if (!monaco) {
      return;
    }

    const model =
      sqlEditorRef.current?.getModel() ??
      monaco.editor.getModel(monaco.Uri.parse(SQL_MODEL_PATH));
    if (!model) {
      return;
    }

    if (!schemaParse.ok || !schemaParse.schema) {
      monaco.editor.setModelMarkers(model, "sqlql", [
        {
          severity: monaco.MarkerSeverity.Error,
          message: "Fix schema JSON before validating SQL.",
          startLineNumber: 1,
          startColumn: 1,
          endLineNumber: 1,
          endColumn: 1,
        },
      ]);
      return;
    }

    const buildErrorMarkers = (messages: string[]) =>
      messages.map((message) => {
        const range = inferSqlErrorRange(sqlText, message);
        if (range) {
          return {
            severity: monaco.MarkerSeverity.Error,
            message,
            ...range,
          };
        }

        return {
          severity: monaco.MarkerSeverity.Error,
          message,
          startLineNumber: 1,
          startColumn: 1,
          endLineNumber: 1,
          endColumn: Math.max(2, model.getLineMaxColumn(1)),
        };
      });

    const compileResult = compilePlaygroundInput(schemaJsonText, rowsJsonText, sqlText);

    if (!compileResult.ok) {
      const messages = compileResult.issues.length > 0 ? compileResult.issues : ["Invalid SQL."];
      monaco.editor.setModelMarkers(model, "sqlql", buildErrorMarkers(messages));
      return;
    }

    if (runtimeError) {
      monaco.editor.setModelMarkers(model, "sqlql", buildErrorMarkers([runtimeError]));
      return;
    }

    monaco.editor.setModelMarkers(model, "sqlql", []);
  }, [rowsJsonText, runtimeError, schemaJsonText, schemaParse, sqlText]);

  useEffect(() => {
    applyJsonSchemas();
  }, [applyJsonSchemas]);

  useEffect(() => {
    if (!schemaParse.ok || !schemaParse.schema) {
      setSelectedSchemaTable(null);
      setSelectedDataTable(null);
      return;
    }

    const tableNames = Object.keys(schemaParse.schema.tables);
    const firstTable = tableNames[0] ?? null;

    setSelectedSchemaTable((current) =>
      current && tableNames.includes(current) ? current : firstTable,
    );
    setSelectedDataTable((current) =>
      current && tableNames.includes(current) ? current : firstTable,
    );
  }, [schemaParse]);

  useEffect(() => {
    setSelectedCatalogQueryId((current) => selectionAfterSchemaChange(current, queryCompatibilityById));
  }, [queryCompatibilityById]);

  useEffect(() => {
    const editor = schemaEditorRef.current;
    const monaco = monacoRef.current;
    if (!editor || !monaco) {
      return;
    }

    if (!selectedSchemaTable) {
      schemaDecorationIdsRef.current = editor.deltaDecorations(schemaDecorationIdsRef.current, []);
      return;
    }

    const line = findTableLineNumber(schemaJsonText, selectedSchemaTable);
    if (!line) {
      schemaDecorationIdsRef.current = editor.deltaDecorations(schemaDecorationIdsRef.current, []);
      return;
    }

    editor.revealLineInCenter(line);
    schemaDecorationIdsRef.current = editor.deltaDecorations(schemaDecorationIdsRef.current, [
      {
        range: new monaco.Range(line, 1, line, 1),
        options: {
          isWholeLine: true,
          className: "schema-table-highlight",
        },
      },
    ]);
  }, [schemaJsonText, selectedSchemaTable]);

  useEffect(() => {
    applySqlMarkers();
  }, [applySqlMarkers]);

  const applyExample = (packId: string): void => {
    const pack = EXAMPLE_PACKS.find((candidate) => candidate.id === packId);
    if (!pack) {
      return;
    }

    const tableNames = Object.keys(pack.schema.tables);
    const firstTable = tableNames[0] ?? null;

    setActivePackId(pack.id);
    setSchemaJsonText(serializeJson(pack.schema));
    setRowsJsonText(serializeJson(pack.rows));
    setSelectedSchemaTable(firstTable);
    setSelectedDataTable(firstTable);
    setSelectedStepId(null);
  };

  const markPresetCustom = (): void => {
    setActivePackId((current) => (current === CUSTOM_PRESET_ID ? current : CUSTOM_PRESET_ID));
  };

  useEffect(() => {
    const requestId = executionRequestIdRef.current + 1;
    executionRequestIdRef.current = requestId;

    setRuntimeError(null);
    setEvents([]);
    setResultRows(null);

    const compileResult = compilePlaygroundInput(schemaJsonText, rowsJsonText, sqlText);
    if (!compileResult.ok) {
      const issueMessage = compileResult.issues[0] ?? "Invalid SQL query.";
      setRuntimeError(issueMessage);
      sessionRef.current = null;
      setPlanSteps([]);
      setPlanScopes([]);
      setSessionTick((tick) => tick + 1);
      return;
    }

    const freshSession = createSession(compileResult);
    sessionRef.current = freshSession;
    const freshPlan = freshSession.getPlan();
    const freshPlanSteps = freshPlan.steps;
    setPlanSteps(freshPlanSteps);
    setPlanScopes(freshPlan.scopes ?? []);
    setSessionTick((tick) => tick + 1);

    void runSessionToCompletion(freshSession, [])
      .then((snapshot) => {
        if (executionRequestIdRef.current !== requestId) {
          return;
        }

        setEvents(snapshot.events);
        setResultRows(snapshot.result);
        setSessionTick((tick) => tick + 1);
      })
      .catch((error: unknown) => {
        if (executionRequestIdRef.current !== requestId) {
          return;
        }

        setRuntimeError(error instanceof Error ? error.message : "Failed to execute query.");
      });
  }, [rowsJsonText, schemaJsonText, sqlText]);

  const handleMonacoMount: OnMount = (editor, monaco) => {
    monacoRef.current = monaco;
    applyJsonSchemas();

    if (!sqlProviderDisposableRef.current) {
      sqlProviderDisposableRef.current = registerSqlCompletionProvider(
        monaco,
        () => schemaForCompletionRef.current,
      );
    }

    const uri = editor.getModel()?.uri.toString();
    if (uri === SQL_MODEL_PATH) {
      sqlEditorRef.current = editor;
      window.requestAnimationFrame(() => {
        recalculateExpandedQueryEditorHeight();
        applySqlMarkers();
      });
    }

    if (uri === SCHEMA_MODEL_PATH) {
      schemaEditorRef.current = editor;
    }
  };

  useEffect(() => {
    if (!isQueryEditorExpanded) {
      return;
    }

    const frameId = window.requestAnimationFrame(() => {
      recalculateExpandedQueryEditorHeight();
    });
    return () => {
      window.cancelAnimationFrame(frameId);
    };
  }, [isQueryEditorExpanded, sqlText, recalculateExpandedQueryEditorHeight]);

  useEffect(() => {
    if (!isQueryEditorExpanded) {
      return;
    }

    const onResize = (): void => {
      recalculateExpandedQueryEditorHeight();
    };
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("resize", onResize);
    };
  }, [isQueryEditorExpanded, recalculateExpandedQueryEditorHeight]);

  useEffect(() => {
    if (!isSchemaSplitDragging) {
      return;
    }

    const onPointerMove = (event: PointerEvent): void => {
      const workspace = schemaWorkspaceRef.current;
      if (!workspace) {
        return;
      }
      if (window.innerWidth < 1024) {
        return;
      }

      const rect = workspace.getBoundingClientRect();
      if (rect.width <= 0) {
        return;
      }

      const rawPercent = ((event.clientX - rect.left) / rect.width) * 100;
      setSchemaSplitPercent(clampSchemaSplitPercent(rawPercent));
    };

    const onPointerUp = (): void => {
      setIsSchemaSplitDragging(false);
    };

    const previousUserSelect = document.body.style.userSelect;
    const previousCursor = document.body.style.cursor;
    document.body.style.userSelect = "none";
    document.body.style.cursor = "col-resize";

    window.addEventListener("pointermove", onPointerMove);
    window.addEventListener("pointerup", onPointerUp);

    return () => {
      document.body.style.userSelect = previousUserSelect;
      document.body.style.cursor = previousCursor;
      window.removeEventListener("pointermove", onPointerMove);
      window.removeEventListener("pointerup", onPointerUp);
    };
  }, [clampSchemaSplitPercent, isSchemaSplitDragging]);

  useEffect(() => {
    if (activeTopTab !== "schema") {
      return;
    }

    const frameId = window.requestAnimationFrame(() => {
      schemaEditorRef.current?.layout();
    });

    return () => {
      window.cancelAnimationFrame(frameId);
    };
  }, [activeTopTab, schemaSplitPercent]);

  useEffect(() => {
    const frameId = window.requestAnimationFrame(() => {
      schemaEditorRef.current?.layout();
      sqlEditorRef.current?.layout();
    });

    return () => {
      window.cancelAnimationFrame(frameId);
    };
  }, [activeTopTab, activeSchemaTab, activeQueryTab, isQueryEditorExpanded, schemaSplitPercent]);

  useEffect(() => {
    if (!isQueryEditorExpanded) {
      return;
    }

    const onPointerDown = (event: PointerEvent): void => {
      const shell = queryEditorShellRef.current;
      if (!shell) {
        return;
      }

      if (shell.contains(event.target as Node)) {
        return;
      }

      setIsQueryEditorExpanded(false);
    };

    const onKeyDown = (event: KeyboardEvent): void => {
      if (event.key === "Escape") {
        setIsQueryEditorExpanded(false);
      }
    };

    window.addEventListener("pointerdown", onPointerDown);
    window.addEventListener("keydown", onKeyDown);

    return () => {
      window.removeEventListener("pointerdown", onPointerDown);
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [isQueryEditorExpanded]);

  useEffect(() => {
    if (activeTopTab !== "schema") {
      setIsSchemaSplitDragging(false);
    }
    if (activeTopTab !== "query") {
      setIsQueryEditorExpanded(false);
    }
  }, [activeTopTab]);

  useEffect(() => {
    return () => {
      sqlProviderDisposableRef.current?.dispose();
      sqlProviderDisposableRef.current = null;
    };
  }, []);

  const handleSelectSchemaTable = (tableName: string): void => {
    setSelectedSchemaTable(tableName);
    setSelectedDataTable(tableName);
  };

  const handleSelectStep = (stepId: string): void => {
    setSelectedStepId(stepId);
  };

  const handleCloseStepOverlay = (): void => {
    setSelectedStepId(null);
  };

  const handleSetTableRows = (tableName: string, tableRows: QueryRow[]): void => {
    if (!schemaParse.ok || !schemaParse.schema) {
      return;
    }

    const merged = mergeTableRows(editableRowsByTable, tableName, tableRows);
    markPresetCustom();
    setRowsJsonText(serializeJson(merged));
  };

  const handleSqlTextChange = (nextValue: string): void => {
    if (nextValue === sqlText) {
      return;
    }

    markPresetCustom();
    setSelectedCatalogQueryId(selectionAfterManualSqlEdit());
    setSqlText(nextValue);
  };

  const handleCatalogQuerySelect = (queryId: string): void => {
    if (queryId === CUSTOM_QUERY_ID) {
      setSelectedCatalogQueryId(CUSTOM_QUERY_ID);
      return;
    }

    if (!canSelectCatalogQuery(queryId, queryCompatibilityById)) {
      return;
    }

    const queryEntry = queryCatalog.find((entry) => entry.id === queryId);
    if (!queryEntry) {
      return;
    }

    setSelectedCatalogQueryId(queryEntry.id);
    setSqlText(queryEntry.sql);
    setIsQueryEditorExpanded(false);
  };

  return (
    <div className="h-screen w-screen overflow-hidden">
      <Tabs
        value={activeTopTab}
        onValueChange={(value) => setActiveTopTab(value as TopTab)}
        className="h-full"
      >
        <div className="flex h-full min-h-0 flex-col">
          <header className="shrink-0 border-b bg-background shadow-sm">
            <div className="grid gap-2 px-2 py-2 lg:grid-cols-[auto_minmax(0,1fr)_auto] lg:items-center">
                <TabsList className="gap-1">
                  <TabsTrigger value="schema" title="Schema" aria-label="Schema" className="px-2.5">
                    <Database className="h-4 w-4" />
                    <span className="sr-only">Schema</span>
                  </TabsTrigger>
                  <TabsTrigger value="data" title="Data" aria-label="Data" className="px-2.5">
                    <Table2 className="h-4 w-4" />
                    <span className="sr-only">Data</span>
                  </TabsTrigger>
                  <TabsTrigger value="query" title="Query" aria-label="Query" className="px-2.5">
                    <SearchCode className="h-4 w-4" />
                    <span className="sr-only">Query</span>
                  </TabsTrigger>
                </TabsList>

                <div className="min-w-0">
                  {activeTopTab === "schema" ? (
                    <Select
                      value={activePack?.id ?? CUSTOM_PRESET_ID}
                      onValueChange={(value) => {
                        if (value === CUSTOM_PRESET_ID) {
                          setActivePackId(CUSTOM_PRESET_ID);
                          return;
                        }
                        applyExample(value);
                      }}
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="Select preset" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value={CUSTOM_PRESET_ID}>Custom</SelectItem>
                        {EXAMPLE_PACKS.map((pack) => (
                          <SelectItem key={pack.id} value={pack.id}>
                            {pack.label}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  ) : null}

                  {activeTopTab === "query" ? (
                    <div className="grid gap-2 lg:grid-cols-[minmax(180px,260px)_minmax(0,1fr)] lg:items-center">
                      <Select
                        value={selectedCatalogQueryId}
                        onValueChange={handleCatalogQuerySelect}
                      >
                        <SelectTrigger>
                          <SelectValue placeholder="Preset query" />
                        </SelectTrigger>
                        <SelectContent className="min-w-[520px]">
                          <SelectItem value={CUSTOM_QUERY_ID}>Custom</SelectItem>
                          <SelectSeparator />
                          {queryCatalogByPack.map((group) => (
                            <SelectGroup key={group.packId}>
                              <SelectLabel>{group.packLabel}</SelectLabel>
                              {group.entries.map((entry) => {
                                const compatibility = queryCompatibilityById[entry.id];
                                const compatible = compatibility?.compatible === true;
                                const reason = compatibility?.reason ?? "Unsupported for this schema.";

                                return (
                                  <SelectItem
                                    key={entry.id}
                                    value={entry.id}
                                    disabled={!compatible}
                                    title={!compatible ? reason : undefined}
                                  >
                                    <div className="flex min-w-0 flex-col">
                                      <span>{`${group.packLabel} · ${entry.queryLabel}`}</span>
                                      {!compatible ? (
                                        <span className="text-xs text-muted-foreground">
                                          {truncateReason(reason)}
                                        </span>
                                      ) : null}
                                    </div>
                                  </SelectItem>
                                );
                              })}
                            </SelectGroup>
                          ))}
                        </SelectContent>
                      </Select>

                      <div ref={queryEditorShellRef} className="relative min-w-0">
                        <SqlPreviewLine
                          monaco={monacoRef.current}
                          sql={sqlText}
                          onActivate={() => setIsQueryEditorExpanded(true)}
                        />
                        {isQueryEditorExpanded ? (
                          <div className="query-editor-overlay absolute inset-x-0 top-0 overflow-visible rounded-md border bg-white px-3 py-2 shadow-2xl">
                            <Editor
                              path={SQL_MODEL_PATH}
                              language="sql"
                              value={sqlText}
                              onMount={handleMonacoMount}
                              onChange={(value) => handleSqlTextChange(value ?? "")}
                              options={{
                                minimap: { enabled: false },
                                fontSize: 13,
                                scrollBeyondLastLine: false,
                                fixedOverflowWidgets: true,
                                suggestOnTriggerCharacters: true,
                                quickSuggestions: {
                                  other: true,
                                  comments: false,
                                  strings: true,
                                },
                                quickSuggestionsDelay: 0,
                              }}
                              height={`${expandedQueryEditorHeightPx}px`}
                            />
                          </div>
                        ) : null}
                      </div>
                    </div>
                  ) : null}
                </div>

                <div className="flex items-center gap-2 justify-self-start lg:justify-self-end">
                  {activeTopTab === "schema" ? (
                    <Tabs
                      value={activeSchemaTab}
                      onValueChange={(value) => setActiveSchemaTab(value as SchemaTab)}
                    >
                      <TabsList className="gap-1">
                        <TabsTrigger value="diagram">Diagram</TabsTrigger>
                        <TabsTrigger value="ddl">DDL</TabsTrigger>
                      </TabsList>
                    </Tabs>
                  ) : null}

                  {activeTopTab === "data" ? (
                    <div className="flex items-center gap-2">
                      <Tabs
                        value={currentDataMode}
                        onValueChange={(value) => {
                          if (!currentDataTable) {
                            return;
                          }
                          setDataEditorMode(value as DataEditorMode);
                        }}
                      >
                        <TabsList className="gap-1">
                          <TabsTrigger value="json" disabled={!currentDataTable}>
                            JSON
                          </TabsTrigger>
                          <TabsTrigger value="grid" disabled={!currentDataTable}>
                            Table
                          </TabsTrigger>
                        </TabsList>
                      </Tabs>
                      <Button
                        type="button"
                        size="sm"
                        disabled={!currentDataTable || !currentDataTableDefinition}
                        onClick={() => {
                          if (!currentDataTable || !currentDataTableDefinition) {
                            return;
                          }
                          handleSetTableRows(
                            currentDataTable,
                            addEmptyRow(currentDataRows, currentDataTableDefinition),
                          );
                        }}
                      >
                        Add row
                      </Button>
                    </div>
                  ) : null}

                  {activeTopTab === "query" ? (
                    <Tabs
                      value={activeQueryTab}
                      onValueChange={(value) => setActiveQueryTab(value as QueryTab)}
                    >
                      <TabsList className="gap-1">
                        <TabsTrigger value="result">Result</TabsTrigger>
                        <TabsTrigger value="explain">Explain</TabsTrigger>
                      </TabsList>
                    </Tabs>
                  ) : null}
                </div>
              </div>
            </header>

          <div className="min-h-0 flex-1 overflow-hidden bg-background p-2">
              <TabsContent value="schema" forceMount className="mt-0 h-full min-h-0">
                <div className="hidden h-full min-h-0 flex-col gap-2 lg:flex">
                  <div
                    ref={schemaWorkspaceRef}
                    className="min-h-0 flex-1 overflow-hidden rounded-md border bg-white lg:grid"
                    style={{ gridTemplateColumns: schemaSplitGridTemplate }}
                  >
                    <div className="min-h-0 overflow-hidden">
                      <Editor
                        path={SCHEMA_MODEL_PATH}
                        language="json"
                        value={schemaJsonText}
                        onMount={handleMonacoMount}
                        onChange={(value) => {
                          const nextValue = value ?? "";
                          if (nextValue !== schemaJsonText) {
                            markPresetCustom();
                            setSchemaJsonText(nextValue);
                          }
                        }}
                        options={{
                          minimap: { enabled: false },
                          fontSize: 13,
                          scrollBeyondLastLine: false,
                        }}
                        height="100%"
                      />
                    </div>

                    <div className="flex min-h-0 items-stretch justify-center">
                      <button
                        type="button"
                        aria-label="Resize schema panels"
                        className="group h-full w-2 cursor-col-resize bg-transparent"
                        onPointerDown={startSchemaSplitDrag}
                      >
                        <span
                          className={cn(
                            "mx-auto block h-full w-px bg-slate-400 transition-colors group-hover:bg-slate-500",
                            isSchemaSplitDragging ? "bg-slate-500" : null,
                          )}
                        />
                      </button>
                    </div>

                    <div className="min-h-0 overflow-hidden">
                      {schemaParse.ok && schemaParse.schema ? (
                        activeSchemaTab === "diagram" ? (
                          <SchemaRelationsGraph
                            schema={schemaParse.schema}
                            selectedTableName={selectedSchemaTable}
                            onSelectTable={handleSelectSchemaTable}
                            onClearSelection={() => setSelectedSchemaTable(null)}
                            heightClassName="h-full"
                            embedded
                          />
                        ) : (
                          <Editor
                            path={SCHEMA_DDL_MODEL_PATH}
                            language="sql"
                            value={ddlText || "Fix schema to generate DDL."}
                            options={{
                              minimap: { enabled: false },
                              fontSize: 13,
                              scrollBeyondLastLine: false,
                              readOnly: true,
                              wordWrap: "off",
                              lineNumbers: "on",
                            }}
                            height="100%"
                          />
                        )
                      ) : (
                        <div className="flex h-full items-center justify-center text-sm text-slate-500">
                          Fix schema JSON to render relations.
                        </div>
                      )}
                    </div>
                  </div>
                  {!schemaParse.ok ? (
                    <Alert variant="warning">
                      <AlertTitle>Schema issues</AlertTitle>
                      <AlertDescription className="whitespace-pre-wrap font-mono text-xs">
                        {schemaParse.issues
                          .map((issue) => `${issue.path}: ${issue.message}`)
                          .join("\n")}
                      </AlertDescription>
                    </Alert>
                  ) : null}
                </div>

                <div className="flex h-full min-h-0 flex-col gap-2 lg:hidden">
                  <div className="min-h-0 flex-1 overflow-hidden rounded-md border">
                    <Editor
                      path={SCHEMA_MODEL_PATH}
                      language="json"
                      value={schemaJsonText}
                      onMount={handleMonacoMount}
                      onChange={(value) => {
                        const nextValue = value ?? "";
                        if (nextValue !== schemaJsonText) {
                          markPresetCustom();
                          setSchemaJsonText(nextValue);
                        }
                      }}
                      options={{
                        minimap: { enabled: false },
                        fontSize: 13,
                        scrollBeyondLastLine: false,
                      }}
                      height="100%"
                    />
                  </div>
                  {!schemaParse.ok ? (
                    <Alert variant="warning">
                      <AlertTitle>Schema issues</AlertTitle>
                      <AlertDescription className="whitespace-pre-wrap font-mono text-xs">
                        {schemaParse.issues
                          .map((issue) => `${issue.path}: ${issue.message}`)
                          .join("\n")}
                      </AlertDescription>
                    </Alert>
                  ) : null}
                  <div className="min-h-0 flex-1 overflow-hidden">
                    {schemaParse.ok && schemaParse.schema ? (
                      activeSchemaTab === "diagram" ? (
                        <SchemaRelationsGraph
                          schema={schemaParse.schema}
                          selectedTableName={selectedSchemaTable}
                          onSelectTable={handleSelectSchemaTable}
                          onClearSelection={() => setSelectedSchemaTable(null)}
                          heightClassName="h-full"
                        />
                      ) : (
                        <div className="h-full overflow-hidden rounded-md border">
                          <Editor
                            path={SCHEMA_DDL_MODEL_PATH}
                            language="sql"
                            value={ddlText || "Fix schema to generate DDL."}
                            options={{
                              minimap: { enabled: false },
                              fontSize: 13,
                              scrollBeyondLastLine: false,
                              readOnly: true,
                              wordWrap: "off",
                              lineNumbers: "on",
                            }}
                            height="100%"
                          />
                        </div>
                      )
                    ) : (
                      <div className="flex h-full items-center justify-center rounded-md border border-dashed text-sm text-slate-500">
                        Fix schema JSON to render relations.
                      </div>
                    )}
                  </div>
                </div>
              </TabsContent>

              <TabsContent value="data" forceMount className="mt-0 h-full min-h-0">
                {schemaParse.ok && schemaParse.schema && schemaTableNames.length > 0 ? (
                  <div className="grid h-full min-h-0 gap-2 lg:grid-cols-[220px_minmax(0,1fr)]">
                    <ScrollArea className="h-full rounded-md border bg-slate-50 p-2">
                      <div className="space-y-1">
                        {schemaTableNames.map((tableName) => (
                          <button
                            type="button"
                            key={tableName}
                            className={cn(
                              "w-full rounded-md px-3 py-2 text-left text-sm",
                              currentDataTable === tableName
                                ? "bg-background text-foreground shadow"
                                : "hover:bg-slate-100",
                            )}
                            onClick={() => setSelectedDataTable(tableName)}
                          >
                            {tableName}
                          </button>
                        ))}
                      </div>
                    </ScrollArea>

                    <div
                      className={cn(
                        "min-h-0 overflow-hidden bg-white",
                        currentDataMode === "json" ? "rounded-md border" : null,
                      )}
                    >
                      {currentDataTable && currentDataTableDefinition ? (
                        currentDataMode === "json" ? (
                          <DataTableJsonEditor
                            tableName={currentDataTable}
                            rows={currentDataRows}
                            onRowsChange={(nextRows) =>
                              handleSetTableRows(currentDataTable, nextRows)
                            }
                            tableValidationIssues={currentTableIssues}
                            className="h-full p-0"
                            editorClassName="min-h-0 flex-1 rounded-none border-0"
                            editorHeight="100%"
                          />
                        ) : (
                          <DataGrid
                            table={currentDataTableDefinition}
                            rows={currentDataRows}
                            onRowsChange={(nextRows) =>
                              handleSetTableRows(currentDataTable, nextRows)
                            }
                            scrollAreaClassName="flex-1 rounded-md border bg-white"
                          />
                        )
                      ) : (
                        <div className="flex h-full items-center justify-center text-sm text-slate-500">
                          Select a table.
                        </div>
                      )}
                    </div>
                  </div>
                ) : (
                  <div className="flex h-full items-center justify-center text-sm text-slate-500">
                    Fix schema JSON to edit table data.
                  </div>
                )}
              </TabsContent>

              <TabsContent value="query" forceMount className="mt-0 h-full min-h-0">
                {activeQueryTab === "result" ? (
                  <div className="h-full min-h-0">
                    {runtimeError ? (
                      <Alert variant="destructive">
                        <AlertTitle>Query rejected</AlertTitle>
                        <AlertDescription className="whitespace-pre-wrap font-mono text-xs">
                          {runtimeError}
                        </AlertDescription>
                      </Alert>
                    ) : resultRows ? (
                      renderRows(resultRows, { heightClassName: "h-full" })
                    ) : (
                      <div className="flex h-full items-center justify-center text-sm text-slate-500">
                        No results yet.
                      </div>
                    )}
                  </div>
                ) : (
                  <div className="relative h-full min-h-0">
                    <PlanGraph
                      steps={planSteps}
                      scopes={planScopes}
                      statesById={statesById}
                      currentStepId={currentStepId}
                      selectedStepId={selectedStepId}
                      isVisible={activeTopTab === "query" && activeQueryTab === "explain"}
                      onSelectStep={handleSelectStep}
                      onClearSelection={handleCloseStepOverlay}
                      heightClassName="h-full"
                    />
                    {selectedStep ? (
                      <div className="pointer-events-none absolute inset-y-4 right-4 z-20 w-[430px] max-w-[48%]">
                        <div className="pointer-events-auto flex h-full flex-col rounded-xl border border-sky-200 bg-white/95 shadow-2xl backdrop-blur-sm">
                          <div className="flex items-start justify-between gap-2 border-b p-3">
                            <div className="min-w-0 space-y-2">
                              <div className="flex flex-wrap items-center gap-2">
                                <Badge variant="secondary" className="font-mono text-[11px]">
                                  {selectedStep.id}
                                </Badge>
                                <Badge variant="outline">{selectedStep.kind}</Badge>
                                <Badge variant="outline">{selectedStep.phase}</Badge>
                                {selectedStep.sqlOrigin ? (
                                  <Badge variant="outline">{selectedStep.sqlOrigin}</Badge>
                                ) : null}
                              </div>
                              <p className="text-sm text-slate-700">{selectedStep.summary}</p>
                            </div>
                            <Button
                              type="button"
                              variant="ghost"
                              size="icon"
                              className="h-7 w-7 shrink-0"
                              onClick={handleCloseStepOverlay}
                            >
                              <X className="h-4 w-4" />
                            </Button>
                          </div>
                          <div className="min-h-0 flex-1 p-3 pt-2">
                            <ScrollArea className="h-full pr-2">
                              <div className="space-y-3">
                                <div className="rounded-md border bg-slate-50 p-3">
                                  <p className="text-xs text-slate-500">
                                    Depends on: {selectedStep.dependsOn.join(", ") || "none"}
                                  </p>
                                </div>

                                <StepSection title="Logical operation" defaultOpen>
                                  <p className="text-xs text-slate-500">
                                    The planner-level intent for this step and the columns it aims to produce.
                                  </p>
                                  <JsonBlock value={selectedStep.operation} />
                                  {selectedStep.outputs && selectedStep.outputs.length > 0 ? (
                                    <div className="text-xs text-slate-600">
                                      Outputs: {selectedStep.outputs.join(", ")}
                                    </div>
                                  ) : null}
                                </StepSection>

                                <StepSection title="Request" defaultOpen>
                                  <p className="text-xs text-slate-500">
                                    The normalized input shape passed into this step at execution time.
                                  </p>
                                  <JsonBlock value={selectedStep.request ?? {}} />
                                </StepSection>

                                <StepSection title="Routing / Pushdown" defaultOpen>
                                  <p className="text-xs text-slate-500">
                                    How work is split between table methods and local engine processing.
                                  </p>
                                  <div className="text-xs text-slate-600">
                                    Route used:{" "}
                                    <span className="font-medium text-slate-900">
                                      {selectedStepState?.routeUsed ?? "pending"}
                                    </span>
                                  </div>
                                  <JsonBlock value={selectedStep.pushdown ?? {}} />
                                  {selectedStepState?.notes &&
                                  selectedStepState.notes.length > 0 ? (
                                    <ul className="list-disc space-y-1 pl-5 text-xs text-slate-600">
                                      {selectedStepState.notes.map((note: string) => (
                                        <li key={note}>{note}</li>
                                      ))}
                                    </ul>
                                  ) : null}
                                </StepSection>

                                <StepSection title="Runtime" defaultOpen>
                                  <p className="text-xs text-slate-500">
                                    Execution status and timing/row-count metrics for this step instance.
                                  </p>
                                  <div className="grid gap-1 text-xs text-slate-600">
                                    <div>Status: {selectedStepState?.status ?? "ready"}</div>
                                    <div>
                                      Execution index:{" "}
                                      {selectedStepState?.executionIndex != null
                                        ? selectedStepState.executionIndex
                                        : "pending"}
                                    </div>
                                    {selectedStepState?.durationMs != null ? (
                                      <div>Duration: {selectedStepState.durationMs}ms</div>
                                    ) : null}
                                    {selectedStepState?.inputRowCount != null ? (
                                      <div>
                                        Input rows: {selectedStepState.inputRowCount}
                                      </div>
                                    ) : null}
                                    {selectedStepState?.outputRowCount != null ? (
                                      <div>
                                        Output rows: {selectedStepState.outputRowCount}
                                      </div>
                                    ) : selectedStepState?.rowCount != null ? (
                                      <div>Output rows: {selectedStepState.rowCount}</div>
                                    ) : null}
                                  </div>
                                  {selectedStepState?.error ? (
                                    <Alert variant="destructive">
                                      <AlertTitle>Step error</AlertTitle>
                                      <AlertDescription>
                                        {selectedStepState.error}
                                      </AlertDescription>
                                    </Alert>
                                  ) : null}
                                </StepSection>

                                {selectedStepState?.rows ? (
                                  <StepSection title="Data preview" defaultOpen={false}>
                                    <p className="text-xs text-slate-500">
                                      Sample output rows emitted by this step after execution.
                                    </p>
                                    {renderRows(selectedStepState.rows, {
                                      heightClassName: "h-[260px]",
                                      expandNestedObjects: true,
                                    })}
                                  </StepSection>
                                ) : null}
                              </div>
                            </ScrollArea>
                          </div>
                        </div>
                      </div>
                    ) : null}
                  </div>
                )}
              </TabsContent>
          </div>
        </div>
      </Tabs>
    </div>
  );
}
