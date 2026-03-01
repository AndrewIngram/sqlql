import { useEffect, useMemo, useState } from "react";
import type React from "react";
import { Trash2 } from "lucide-react";
import type { QueryRow, TableColumnDefinition, TableDefinition } from "sqlql";

import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import {
  addEmptyRow,
  coerceCellInput,
  deleteRow,
  formatCellValue,
  updateRowCell,
} from "@/data-editing";
import { isColumnNullable, readColumnType } from "@/types";

interface DataGridProps {
  tableName: string;
  table: TableDefinition;
  rows: QueryRow[];
  onRowsChange(rows: QueryRow[]): void;
}

function cellKey(rowIndex: number, columnName: string): string {
  return `${rowIndex}:${columnName}`;
}

export function DataGrid({ tableName, table, rows, onRowsChange }: DataGridProps): React.JSX.Element {
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
  }, [rows.length, tableName]);

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
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <div className="text-sm font-medium text-slate-700">{tableName}</div>
        <Button type="button" size="sm" onClick={() => onRowsChange(addEmptyRow(rows, table))}>
          Add row
        </Button>
      </div>

      <ScrollArea className="h-[520px] rounded-md border bg-white">
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
              <TableHead className="w-12 text-right"> </TableHead>
            </TableRow>
          </TableHeader>

          <TableBody>
            {rows.length === 0 ? (
              <TableRow>
                <TableCell className="text-sm text-slate-500" colSpan={columns.length + 1}>
                  No rows.
                </TableCell>
              </TableRow>
            ) : null}

            {rows.map((row, rowIndex) => (
              <TableRow key={`row-${rowIndex}`}>
                {columns.map(([columnName, columnDefinition]) => {
                  const key = cellKey(rowIndex, columnName);
                  const draftValue = drafts[key];
                  const currentValue = row[columnName];
                  const displayValue = draftValue ?? formatCellValue(currentValue);
                  const error = errors[key];
                  const type = readColumnType(columnDefinition);

                  return (
                    <TableCell key={columnName} className="align-top">
                      {type === "boolean" ? (
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
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </ScrollArea>
    </div>
  );
}
