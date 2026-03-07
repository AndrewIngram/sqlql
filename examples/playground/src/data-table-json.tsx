import { useEffect, useMemo, useState } from "react";
import type React from "react";
import Editor from "@monaco-editor/react";
import type { QueryRow } from "../../../src/index";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { serializeJson } from "@/examples";
import { cn } from "@/lib/utils";

interface DataTableJsonEditorProps {
  tableName: string;
  rows: QueryRow[];
  onRowsChange(rows: QueryRow[]): void;
  tableValidationIssues: string[];
  className?: string;
  editorClassName?: string;
  editorHeight?: string;
}

export function DataTableJsonEditor({
  tableName,
  rows,
  onRowsChange,
  tableValidationIssues,
  className,
  editorClassName,
  editorHeight = "520px",
}: DataTableJsonEditorProps): React.JSX.Element {
  const externalRowsText = useMemo(() => serializeJson(rows), [rows]);
  const [text, setText] = useState(externalRowsText);
  const [parseIssue, setParseIssue] = useState<string | null>(null);

  useEffect(() => {
    setText(externalRowsText);
    setParseIssue(null);
  }, [externalRowsText, tableName]);

  const handleChange = (nextText: string): void => {
    setText(nextText);

    try {
      const parsed = JSON.parse(nextText);
      if (!Array.isArray(parsed)) {
        setParseIssue("Table JSON must be an array of row objects.");
        return;
      }

      const hasNonObject = parsed.some(
        (entry) => typeof entry !== "object" || entry == null || Array.isArray(entry),
      );
      if (hasNonObject) {
        setParseIssue("Each row must be a JSON object.");
        return;
      }

      setParseIssue(null);
      onRowsChange(parsed as QueryRow[]);
    } catch (error) {
      setParseIssue(error instanceof Error ? error.message : "Invalid JSON.");
    }
  };

  return (
    <div className={cn("flex min-h-0 flex-col gap-3", className)}>
      <div className={cn("min-h-0 overflow-hidden rounded-md border", editorClassName)}>
        <Editor
          path={`inmemory://sqlql/data-table-${tableName}.json`}
          language="json"
          value={text}
          onChange={(value) => handleChange(value ?? "")}
          options={{ minimap: { enabled: false }, fontSize: 13, scrollBeyondLastLine: false }}
          height={editorHeight}
        />
      </div>

      {parseIssue ? (
        <Alert variant="warning">
          <AlertTitle>JSON issues</AlertTitle>
          <AlertDescription className="font-mono text-xs">{parseIssue}</AlertDescription>
        </Alert>
      ) : null}

      {tableValidationIssues.length > 0 ? (
        <Alert variant="warning">
          <AlertTitle>Table validation issues</AlertTitle>
          <AlertDescription className="whitespace-pre-wrap font-mono text-xs">
            {tableValidationIssues.join("\n")}
          </AlertDescription>
        </Alert>
      ) : null}
    </div>
  );
}
