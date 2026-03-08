import { useEffect, useMemo, useState } from "react";
import type React from "react";
import { Trash2 } from "lucide-react";
import type { QueryRow, TableColumnDefinition, TableDefinition } from "@tupl/schema";

import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { coerceCellInput, deleteRow, formatCellValue, updateRowCell } from "@/data-editing";
import { isColumnNullable, readColumnEnumValues, readColumnType } from "@/types";
import { cn } from "@/lib/utils";

interface DataGridProps {
  table: TableDefinition;
  rows: QueryRow[];
  onRowsChange(rows: QueryRow[]): void;
  scrollAreaClassName?: string;
  selectedRowIndex?: number | null;
  onSelectRow?(rowIndex: number): void;
  editable?: boolean;
}

function cellKey(rowIndex: number, columnName: string): string {
  return `${rowIndex}:${columnName}`;
}

function toDateTimeLocalValue(value: unknown): string {
  if (typeof value !== "string") {
    return "";
  }

  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return "";
  }

  const dateOnly = /^(\d{4}-\d{2}-\d{2})$/u.exec(trimmed);
  if (dateOnly?.[1]) {
    return `${dateOnly[1]}T00:00`;
  }

  const dateTime = /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})(?::\d{2}(?:\.\d+)?)?(?:Z)?$/u.exec(
    trimmed,
  );
  if (dateTime?.[1] && dateTime?.[2]) {
    return `${dateTime[1]}T${dateTime[2]}`;
  }

  return "";
}

export function DataGrid({
  table,
  rows,
  onRowsChange,
  scrollAreaClassName,
  selectedRowIndex,
  onSelectRow,
  editable = true,
}: DataGridProps): React.JSX.Element {
  const columns = useMemo(() => Object.entries(table.columns), [table.columns]);
  const [drafts, setDrafts] = useState<Record<string, string>>({});
  const [errors, setErrors] = useState<Record<string, string>>({});

  useEffect(() => {
    const maxRowIndex = rows.length - 1;

    setDrafts((previous) => {
      const next = Object.fromEntries(
        Object.entries(previous).filter(([key]) => {
          const [rowIndexText] = key.split(":");
          const rowIndex = Number(rowIndexText);
          return Number.isFinite(rowIndex) && rowIndex <= maxRowIndex;
        }),
      );

      return next;
    });

    setErrors((previous) => {
      const next = Object.fromEntries(
        Object.entries(previous).filter(([key]) => {
          const [rowIndexText] = key.split(":");
          const rowIndex = Number(rowIndexText);
          return Number.isFinite(rowIndex) && rowIndex <= maxRowIndex;
        }),
      );

      return next;
    });
  }, [rows.length]);

  const commitCellText = (
    rowIndex: number,
    columnName: string,
    columnDefinition: TableColumnDefinition,
    raw: string,
  ): void => {
    const key = cellKey(rowIndex, columnName);
    setDrafts((previous) => ({ ...previous, [key]: raw }));

    const coercion = coerceCellInput(columnDefinition, raw);
    if (!coercion.ok) {
      setErrors((previous) => ({ ...previous, [key]: coercion.error }));
      return;
    }

    setErrors((previous) => {
      if (!(key in previous)) {
        return previous;
      }

      const next = { ...previous };
      delete next[key];
      return next;
    });

    setDrafts((previous) => {
      if (!(key in previous)) {
        return previous;
      }

      const next = { ...previous };
      delete next[key];
      return next;
    });

    onRowsChange(updateRowCell(rows, rowIndex, columnName, coercion.value));
  };

  return (
    <div className="flex min-h-0 flex-col">
      <ScrollArea className={cn(scrollAreaClassName ?? "h-[520px]", "min-h-0")}>
        <Table>
          <TableHeader>
            <TableRow>
              {columns.map(([columnName, columnDefinition]) => (
                <TableHead key={columnName} className="min-w-44 align-bottom">
                  <div className="font-semibold">{columnName}</div>
                  <div className="text-[11px] font-normal text-slate-500">
                    {readColumnType(columnDefinition)}
                    {isColumnNullable(columnDefinition) ? " | nullable" : ""}
                  </div>
                </TableHead>
              ))}
              {editable ? <TableHead className="w-12 text-right"> </TableHead> : null}
            </TableRow>
          </TableHeader>

          <TableBody>
            {rows.length === 0 ? (
              <TableRow>
                <TableCell
                  className="text-sm text-slate-500"
                  colSpan={columns.length + (editable ? 1 : 0)}
                >
                  No rows.
                </TableCell>
              </TableRow>
            ) : null}

            {rows.map((row, rowIndex) => (
              <TableRow
                key={`row-${rowIndex}`}
                className={cn(
                  onSelectRow ? "cursor-pointer" : null,
                  selectedRowIndex === rowIndex ? "bg-sky-50/60 hover:bg-sky-100/60" : null,
                )}
                onClick={() => onSelectRow?.(rowIndex)}
              >
                {columns.map(([columnName, columnDefinition]) => {
                  const key = cellKey(rowIndex, columnName);
                  const draftValue = drafts[key];
                  const currentValue = row[columnName];
                  const displayValue = draftValue ?? formatCellValue(currentValue);
                  const error = errors[key];
                  const type = readColumnType(columnDefinition);
                  const enumValues = readColumnEnumValues(columnDefinition) ?? [];
                  const timestampValue = draftValue ?? toDateTimeLocalValue(currentValue);
                  const timestampFallback =
                    draftValue == null &&
                    typeof currentValue === "string" &&
                    currentValue.length > 0 &&
                    timestampValue.length === 0;

                  return (
                    <TableCell key={columnName} className="align-top">
                      {!editable ? (
                        <div className="font-mono text-xs text-slate-700">
                          {JSON.stringify(currentValue ?? null)}
                        </div>
                      ) : enumValues.length > 0 ? (
                        <select
                          className="h-8 w-full rounded-md border border-slate-200 bg-white px-2 text-xs"
                          value={currentValue == null ? "__null__" : String(currentValue)}
                          onChange={(event) =>
                            commitCellText(
                              rowIndex,
                              columnName,
                              columnDefinition,
                              event.target.value === "__null__" ? "" : event.target.value,
                            )
                          }
                        >
                          {isColumnNullable(columnDefinition) ? (
                            <option value="__null__"></option>
                          ) : null}
                          {enumValues.map((value) => (
                            <option key={value} value={value}>
                              {value}
                            </option>
                          ))}
                        </select>
                      ) : type === "boolean" ? (
                        <select
                          className="h-8 w-full rounded-md border border-slate-200 bg-white px-2 text-xs"
                          value={
                            currentValue == null
                              ? "__null__"
                              : currentValue === true
                                ? "true"
                                : "false"
                          }
                          onChange={(event) =>
                            commitCellText(
                              rowIndex,
                              columnName,
                              columnDefinition,
                              event.target.value === "__null__" ? "" : event.target.value,
                            )
                          }
                        >
                          {isColumnNullable(columnDefinition) ? (
                            <option value="__null__"></option>
                          ) : null}
                          <option value="true">true</option>
                          <option value="false">false</option>
                        </select>
                      ) : type === "integer" ? (
                        <input
                          type="number"
                          step={1}
                          inputMode="numeric"
                          className="h-8 w-full rounded-md border border-slate-200 px-2 font-mono text-xs"
                          value={displayValue}
                          onChange={(event) =>
                            commitCellText(
                              rowIndex,
                              columnName,
                              columnDefinition,
                              event.target.value,
                            )
                          }
                        />
                      ) : type === "timestamp" && !timestampFallback ? (
                        <input
                          type="datetime-local"
                          className="h-8 w-full rounded-md border border-slate-200 px-2 font-mono text-xs"
                          value={timestampValue}
                          onChange={(event) =>
                            commitCellText(
                              rowIndex,
                              columnName,
                              columnDefinition,
                              event.target.value,
                            )
                          }
                        />
                      ) : (
                        <input
                          className="h-8 w-full rounded-md border border-slate-200 px-2 font-mono text-xs"
                          value={displayValue}
                          onChange={(event) =>
                            commitCellText(
                              rowIndex,
                              columnName,
                              columnDefinition,
                              event.target.value,
                            )
                          }
                        />
                      )}
                      {error ? <div className="mt-1 text-[11px] text-red-600">{error}</div> : null}
                    </TableCell>
                  );
                })}
                {editable ? (
                  <TableCell className="align-top text-right">
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      className="h-8 w-8"
                      onClick={() => onRowsChange(deleteRow(rows, rowIndex))}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </TableCell>
                ) : null}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </ScrollArea>
    </div>
  );
}
