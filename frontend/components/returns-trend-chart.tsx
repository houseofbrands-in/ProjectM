"use client";

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";

function labelMap(key: string) {
  if (key === "returns_units") return "Total Returns";
  if (key === "return_units") return "Return";
  if (key === "rto_units") return "RTO";
  return key;
}

export function ReturnsTrendChart({ data }: { data: any[] }) {
  return (
    <div className="h-[260px] w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="date" tickMargin={8} minTickGap={24} />
          <YAxis tickMargin={8} />
          <Tooltip
            formatter={(value: any, name: any) => [value, labelMap(String(name))]}
          />

          {/* Keep it clean for clients */}
          <Line
            type="monotone"
            dataKey="returns_units"
            name="Total Returns"
            dot={false}
          />
          <Line type="monotone" dataKey="rto_units" name="RTO" dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
