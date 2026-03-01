import { useEffect, useMemo, useState } from "react";
import type React from "react";
import type * as Monaco from "monaco-editor";

import { normalizeSqlPreview } from "./query-preview";

interface SqlPreviewLineProps {
  monaco: typeof Monaco | null;
  sql: string;
  onActivate: () => void;
}

export function SqlPreviewLine({ monaco, sql, onActivate }: SqlPreviewLineProps): React.JSX.Element {
  const normalized = useMemo(() => normalizeSqlPreview(sql), [sql]);
  const [colorizedHtml, setColorizedHtml] = useState<string>("");

  useEffect(() => {
    let cancelled = false;

    if (!monaco || normalized.length === 0) {
      setColorizedHtml("");
      return () => {
        cancelled = true;
      };
    }

    void monaco.editor
      .colorize(normalized, "sql", {})
      .then((result) => {
        if (cancelled) {
          return;
        }
        setColorizedHtml(result);
      })
      .catch(() => {
        if (cancelled) {
          return;
        }
        setColorizedHtml("");
      });

    return () => {
      cancelled = true;
    };
  }, [monaco, normalized]);

  const previewText = normalized.length > 0 ? normalized : "Type a SQL query";

  return (
    <button
      type="button"
      onClick={onActivate}
      className="sql-preview-line w-full rounded-md border border-input bg-white px-3 py-2 text-left font-mono text-sm text-slate-900 shadow-sm transition-colors hover:bg-slate-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
      title={previewText}
      aria-label="Edit SQL query"
    >
      {colorizedHtml ? (
        <span
          className="sql-preview-line__content"
          // Monaco colorize output is escaped/tokenized HTML.
          dangerouslySetInnerHTML={{ __html: colorizedHtml }}
        />
      ) : (
        <span className="sql-preview-line__content text-slate-700">{previewText}</span>
      )}
    </button>
  );
}

