import { useEffect, useMemo, useRef, useState } from "react";
import type React from "react";
import Editor, { type OnMount } from "@monaco-editor/react";
import { ChevronDown, ChevronRight, X } from "lucide-react";
import type * as Monaco from "monaco-editor";
import type {
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
import { mergeTableRows } from "@/data-editing";
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
  parseRowsText,
  parseSchemaText,
} from "@/validation";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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
import { Separator } from "@/components/ui/separator";
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

type TopTab = "schema" | "data" | "query";
type SchemaTab = "diagram" | "ddl";
type QueryTab = "result" | "explain";
type DataEditorMode = "json" | "grid";

function formatIssues(issues: string[]): string {
  return issues.join("\n");
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
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
  const [dataEditorModeByTable, setDataEditorModeByTable] = useState<
    Record<string, DataEditorMode>
  >(() => {
    if (!defaultPack) {
      return {};
    }

    return Object.fromEntries(
      Object.keys(defaultPack.schema.tables).map((tableName) => [tableName, "json"]),
    );
  });

  const [runtimeError, setRuntimeError] = useState<string | null>(null);
  const [planSteps, setPlanSteps] = useState<QueryExecutionPlanStep[]>([]);
  const [events, setEvents] = useState<QueryStepEvent[]>([]);
  const [resultRows, setResultRows] = useState<Array<Record<string, unknown>> | null>(null);
  const [selectedStepId, setSelectedStepId] = useState<string | null>(null);
  const [selectedCatalogQueryId, setSelectedCatalogQueryId] = useState(defaultCatalogId);
  const [isQueryEditorExpanded, setIsQueryEditorExpanded] = useState(false);
  const [sessionTick, setSessionTick] = useState(0);

  const monacoRef = useRef<typeof Monaco | null>(null);
  const schemaEditorRef = useRef<Monaco.editor.IStandaloneCodeEditor | null>(null);
  const sqlEditorRef = useRef<Monaco.editor.IStandaloneCodeEditor | null>(null);
  const sqlProviderDisposableRef = useRef<Monaco.IDisposable | null>(null);
  const schemaDecorationIdsRef = useRef<string[]>([]);
  const queryEditorShellRef = useRef<HTMLDivElement | null>(null);

  const sessionRef = useRef<QuerySession | null>(null);
  const executionRequestIdRef = useRef(0);
  const schemaForCompletionRef = useRef<SchemaDefinition | null>(null);

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
  const currentDataMode: DataEditorMode =
    (currentDataTable ? dataEditorModeByTable[currentDataTable] : undefined) ?? "json";

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

  useEffect(() => {
    schemaForCompletionRef.current = schemaParse.ok ? schemaParse.schema ?? null : null;

    if (monacoRef.current) {
      monacoRef.current.languages.json.jsonDefaults.setDiagnosticsOptions({
        validate: true,
        allowComments: false,
        schemas: [
          {
            uri: "sqlql://schema-format",
            fileMatch: [SCHEMA_MODEL_PATH],
            schema: PLAYGROUND_SCHEMA_JSON_SCHEMA,
          },
        ],
      });
    }
  }, [schemaParse]);

  useEffect(() => {
    if (!schemaParse.ok || !schemaParse.schema) {
      setSelectedSchemaTable(null);
      setSelectedDataTable(null);
      setDataEditorModeByTable({});
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
    setDataEditorModeByTable((current) => {
      const next: Record<string, DataEditorMode> = {};
      for (const tableName of tableNames) {
        next[tableName] = current[tableName] ?? "json";
      }
      return next;
    });
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
    const monaco = monacoRef.current;
    const sqlEditor = sqlEditorRef.current;
    if (!monaco || !sqlEditor) {
      return;
    }

    const model = sqlEditor.getModel();
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

    const compileResult = compilePlaygroundInput(schemaJsonText, rowsJsonText, sqlText);

    if (!compileResult.ok) {
      monaco.editor.setModelMarkers(model, "sqlql", [
        {
          severity: monaco.MarkerSeverity.Error,
          message: compileResult.issues[0] ?? "Invalid SQL.",
          startLineNumber: 1,
          startColumn: 1,
          endLineNumber: 1,
          endColumn: Math.max(2, model.getLineMaxColumn(1)),
        },
      ]);
      return;
    }

    monaco.editor.setModelMarkers(model, "sqlql", []);
  }, [rowsJsonText, schemaJsonText, schemaParse, sqlText]);

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
    setDataEditorModeByTable(
      Object.fromEntries(tableNames.map((tableName) => [tableName, "json"])),
    );
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
      sessionRef.current = null;
      setPlanSteps([]);
      setSessionTick((tick) => tick + 1);
      setRuntimeError(formatIssues(compileResult.issues));
      return;
    }

    const freshSession = createSession(compileResult);
    sessionRef.current = freshSession;
    const freshPlanSteps = freshSession.getPlan().steps;
    setPlanSteps(freshPlanSteps);
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

    if (!sqlProviderDisposableRef.current) {
      sqlProviderDisposableRef.current = registerSqlCompletionProvider(
        monaco,
        () => schemaForCompletionRef.current,
      );
    }

    const uri = editor.getModel()?.uri.toString();
    if (uri === SQL_MODEL_PATH) {
      sqlEditorRef.current = editor;
    }

    if (uri === SCHEMA_MODEL_PATH) {
      schemaEditorRef.current = editor;
    }
  };

  useEffect(() => {
    const frameId = window.requestAnimationFrame(() => {
      schemaEditorRef.current?.layout();
      sqlEditorRef.current?.layout();
    });

    return () => {
      window.cancelAnimationFrame(frameId);
    };
  }, [activeTopTab, activeSchemaTab, activeQueryTab, isQueryEditorExpanded]);

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
    <div className="min-h-screen bg-[radial-gradient(circle_at_top_right,_#e0f2fe_0,_#f8fafc_45%,_#eef2ff_100%)] px-4 py-4 lg:px-6">
      <div className="mx-auto max-w-[1800px] space-y-4">
        {runtimeError ? (
          <Alert variant="destructive">
            <AlertTitle>Runtime error</AlertTitle>
            <AlertDescription className="whitespace-pre-wrap font-mono text-xs">
              {runtimeError}
            </AlertDescription>
          </Alert>
        ) : null}

        <Tabs value={activeTopTab} onValueChange={(value) => setActiveTopTab(value as TopTab)}>
          <TabsList>
            <TabsTrigger value="schema">Schema</TabsTrigger>
            <TabsTrigger value="data">Data</TabsTrigger>
            <TabsTrigger value="query">Query</TabsTrigger>
          </TabsList>

          <TabsContent value="schema" forceMount className="mt-4">
              <div className="grid gap-4 xl:grid-cols-[minmax(420px,1fr)_minmax(420px,1fr)]">
                <Card>
                  <CardHeader>
                    <CardTitle>Schema JSON</CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div className="space-y-1">
                      <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                        Presets
                      </div>
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
                    </div>
                    <Separator />
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
                      options={{ minimap: { enabled: false }, fontSize: 13 }}
                      height="640px"
                    />
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
                  </CardContent>
                </Card>

                <Card>
                  <CardContent className="pt-4">
                    <Tabs
                      value={activeSchemaTab}
                      onValueChange={(value) => setActiveSchemaTab(value as SchemaTab)}
                    >
                      <TabsList>
                        <TabsTrigger value="diagram">Diagram</TabsTrigger>
                        <TabsTrigger value="ddl">DDL</TabsTrigger>
                      </TabsList>

                      <TabsContent value="diagram" forceMount className="mt-4">
                          {schemaParse.ok && schemaParse.schema ? (
                            <SchemaRelationsGraph
                              schema={schemaParse.schema}
                              selectedTableName={selectedSchemaTable}
                              onSelectTable={handleSelectSchemaTable}
                              onClearSelection={() => setSelectedSchemaTable(null)}
                              heightClassName="h-[680px]"
                            />
                          ) : (
                            <div className="text-sm text-slate-500">
                              Fix schema JSON to render relations.
                            </div>
                          )}
                      </TabsContent>

                      <TabsContent value="ddl" forceMount className="mt-4">
                          <Editor
                            path={SCHEMA_DDL_MODEL_PATH}
                            language="sql"
                            value={ddlText || "Fix schema to generate DDL."}
                            options={{
                              minimap: { enabled: false },
                              fontSize: 13,
                              readOnly: true,
                              wordWrap: "off",
                              lineNumbers: "on",
                            }}
                            height="680px"
                          />
                      </TabsContent>
                    </Tabs>
                  </CardContent>
                </Card>
              </div>
          </TabsContent>

          <TabsContent value="data" forceMount className="mt-4">
              <Card>
                <CardContent className="pt-4">
                  {schemaParse.ok && schemaParse.schema && schemaTableNames.length > 0 ? (
                    <div className="grid gap-4 xl:grid-cols-[220px_minmax(0,1fr)]">
                      <Card>
                        <CardHeader>
                          <CardTitle>Tables</CardTitle>
                        </CardHeader>
                        <CardContent className="p-2">
                          <ScrollArea className="h-[620px]">
                            <div className="space-y-1">
                              {schemaTableNames.map((tableName) => (
                                <button
                                  type="button"
                                  key={tableName}
                                  className={cn(
                                    "w-full rounded-md px-3 py-2 text-left text-sm",
                                    currentDataTable === tableName
                                      ? "bg-sky-100 text-sky-900"
                                      : "hover:bg-slate-100",
                                  )}
                                  onClick={() => setSelectedDataTable(tableName)}
                                >
                                  {tableName}
                                </button>
                              ))}
                            </div>
                          </ScrollArea>
                        </CardContent>
                      </Card>

                      <div className="space-y-4">
                        <Card>
                          <CardHeader className="pb-2">
                            <div className="flex flex-wrap items-center justify-between gap-2">
                              <CardTitle>{currentDataTable ?? "Table data"}</CardTitle>
                              {currentDataTable ? (
                                <div className="flex gap-2">
                                  <Button
                                    type="button"
                                    size="sm"
                                    variant={currentDataMode === "json" ? "default" : "secondary"}
                                    onClick={() =>
                                      setDataEditorModeByTable((previous) => ({
                                        ...previous,
                                        [currentDataTable]: "json",
                                      }))
                                    }
                                  >
                                    JSON
                                  </Button>
                                  <Button
                                    type="button"
                                    size="sm"
                                    variant={currentDataMode === "grid" ? "default" : "secondary"}
                                    onClick={() =>
                                      setDataEditorModeByTable((previous) => ({
                                        ...previous,
                                        [currentDataTable]: "grid",
                                      }))
                                    }
                                  >
                                    Grid
                                  </Button>
                                </div>
                              ) : null}
                            </div>
                          </CardHeader>

                          <CardContent>
                            {currentDataTable && currentDataTableDefinition ? (
                              currentDataMode === "json" ? (
                                <DataTableJsonEditor
                                  tableName={currentDataTable}
                                  rows={currentDataRows}
                                  onRowsChange={(nextRows) =>
                                    handleSetTableRows(currentDataTable, nextRows)
                                  }
                                  tableValidationIssues={currentTableIssues}
                                />
                              ) : (
                                <DataGrid
                                  tableName={currentDataTable}
                                  table={currentDataTableDefinition}
                                  rows={currentDataRows}
                                  onRowsChange={(nextRows) =>
                                    handleSetTableRows(currentDataTable, nextRows)
                                  }
                                />
                              )
                            ) : (
                              <div className="text-sm text-slate-500">Select a table.</div>
                            )}
                          </CardContent>
                        </Card>
                      </div>
                    </div>
                  ) : (
                    <div className="text-sm text-slate-500">Fix schema JSON to edit table data.</div>
                  )}
                </CardContent>
              </Card>
          </TabsContent>

          <TabsContent value="query" forceMount className="mt-4">
            <Card>
              <CardContent className="pt-4">
                <Tabs
                  value={activeQueryTab}
                  onValueChange={(value) => setActiveQueryTab(value as QueryTab)}
                >
                  <div className="grid gap-2 lg:grid-cols-[minmax(170px,220px)_minmax(0,1fr)_auto] lg:items-center">
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
                        <div className="query-editor-overlay absolute inset-x-0 top-0 overflow-hidden rounded-md border bg-white shadow-2xl">
                          <Editor
                            path={SQL_MODEL_PATH}
                            language="sql"
                            value={sqlText}
                            onMount={handleMonacoMount}
                            onChange={(value) => handleSqlTextChange(value ?? "")}
                            options={{ minimap: { enabled: false }, fontSize: 13 }}
                            height="300px"
                          />
                        </div>
                      ) : null}
                    </div>

                    <TabsList className="justify-self-end">
                      <TabsTrigger value="result">Result</TabsTrigger>
                      <TabsTrigger value="explain">Explain</TabsTrigger>
                    </TabsList>
                  </div>

                  <TabsContent value="result" forceMount className="mt-4">
                    <Card>
                      <CardHeader>
                        <CardTitle>Query result</CardTitle>
                      </CardHeader>
                      <CardContent>
                        {resultRows ? (
                          renderRows(resultRows)
                        ) : (
                          <div className="text-sm text-slate-500">No results yet.</div>
                        )}
                      </CardContent>
                    </Card>
                  </TabsContent>

                  <TabsContent value="explain" forceMount className="mt-4">
                    <Card>
                      <CardHeader>
                        <CardTitle>Execution plan</CardTitle>
                      </CardHeader>
                      <CardContent>
                        <div className="relative">
                          <PlanGraph
                            steps={planSteps}
                            statesById={statesById}
                            currentStepId={currentStepId}
                            selectedStepId={selectedStepId}
                            isVisible={activeTopTab === "query" && activeQueryTab === "explain"}
                            onSelectStep={handleSelectStep}
                            onClearSelection={handleCloseStepOverlay}
                            heightClassName="h-[680px]"
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
                      </CardContent>
                    </Card>
                  </TabsContent>
                </Tabs>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
