export type TuplDiagnosticSeverity = "error" | "warning" | "note";
export type TuplDiagnosticClass = "0A000" | "22000" | "42000" | "54000" | "57000" | "58000";

export interface TuplDiagnostic {
  code: string;
  class: TuplDiagnosticClass;
  severity: TuplDiagnosticSeverity;
  message: string;
  details?: Record<string, unknown>;
}
