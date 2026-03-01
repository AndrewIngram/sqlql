export function normalizeSqlPreview(sql: string): string {
  return sql.replace(/\s+/gu, " ").trim();
}

export function truncateReason(reason: string, maxLength = 96): string {
  if (reason.length <= maxLength) {
    return reason;
  }

  return `${reason.slice(0, Math.max(0, maxLength - 1)).trimEnd()}…`;
}

