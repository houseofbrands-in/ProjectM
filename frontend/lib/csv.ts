// frontend/lib/csv.ts
export type CsvColumn<T> = {
  key: keyof T | string;
  header: string;
  // optional custom getter (useful when key doesn't exist or needs formatting)
  get?: (row: T) => any;
};

function esc(v: any) {
  const s = String(v ?? "");
  if (s.includes(",") || s.includes('"') || s.includes("\n")) {
    return `"${s.replaceAll('"', '""')}"`;
  }
  return s;
}

export function toCSV<T>(rows: T[], columns: CsvColumn<T>[]) {
  const headerLine = columns.map((c) => esc(c.header)).join(",");
  const lines = rows.map((r) =>
    columns
      .map((c) => {
        const val = c.get ? c.get(r) : (r as any)[c.key as any];
        return esc(val);
      })
      .join(",")
  );
  return [headerLine, ...lines].join("\n");
}

export function downloadBlob(filename: string, blob: Blob) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}
