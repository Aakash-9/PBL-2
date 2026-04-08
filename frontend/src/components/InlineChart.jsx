import { useState } from "react";
import {
  BarChart, Bar, LineChart, Line, AreaChart, Area,
  PieChart, Pie, Cell, XAxis, YAxis, Tooltip, Legend,
  ResponsiveContainer, CartesianGrid
} from "recharts";

const COLORS = ["#3b82f6", "#22c55e", "#f59e0b", "#ef4444", "#a855f7", "#06b6d4", "#f97316"];
const TYPES = ["bar", "line", "area", "pie"];

function detectBestChart(rows) {
  if (!rows?.length) return "bar";
  const keys = Object.keys(rows[0]);
  const vals = Object.values(rows[0]);
  const hasDate = keys.some(k => k.toLowerCase().includes("date") || k.toLowerCase().includes("month") || k.toLowerCase().includes("year"));
  const numericCount = vals.filter(v => !isNaN(parseFloat(v))).length;
  if (hasDate) return "line";
  if (rows.length <= 6 && numericCount === 1) return "pie";
  return "bar";
}

export default function InlineChart({ rows }) {
  const keys = Object.keys(rows[0]);
  const xKey = keys[0];
  const yKeys = keys.slice(1).filter(k => rows.some(r => !isNaN(parseFloat(r[k]))));
  const [chartType, setChartType] = useState(detectBestChart(rows));

  const data = rows.slice(0, 50).map(r => {
    const obj = { [xKey]: String(r[xKey] ?? "") };
    yKeys.forEach(k => { obj[k] = parseFloat(r[k]) || 0; });
    return obj;
  });

  function renderChart() {
    if (chartType === "pie") {
      const pieData = data.slice(0, 8).map(r => ({ name: r[xKey], value: r[yKeys[0]] || 0 }));
      return (
        <ResponsiveContainer width="100%" height={280}>
          <PieChart>
            <Pie data={pieData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={100} label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}>
              {pieData.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
            </Pie>
            <Tooltip formatter={(v) => v.toLocaleString()} />
          </PieChart>
        </ResponsiveContainer>
      );
    }
    if (chartType === "line") {
      return (
        <ResponsiveContainer width="100%" height={280}>
          <LineChart data={data}><CartesianGrid strokeDasharray="3 3" stroke="#2a2d3a" />
            <XAxis dataKey={xKey} tick={{ fill: "#94a3b8", fontSize: 11 }} />
            <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} />
            <Tooltip contentStyle={{ background: "#1e2130", border: "1px solid #2a2d3a", borderRadius: 8 }} />
            <Legend />
            {yKeys.map((k, i) => <Line key={k} type="monotone" dataKey={k} stroke={COLORS[i % COLORS.length]} strokeWidth={2} dot={false} />)}
          </LineChart>
        </ResponsiveContainer>
      );
    }
    if (chartType === "area") {
      return (
        <ResponsiveContainer width="100%" height={280}>
          <AreaChart data={data}><CartesianGrid strokeDasharray="3 3" stroke="#2a2d3a" />
            <XAxis dataKey={xKey} tick={{ fill: "#94a3b8", fontSize: 11 }} />
            <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} />
            <Tooltip contentStyle={{ background: "#1e2130", border: "1px solid #2a2d3a", borderRadius: 8 }} />
            <Legend />
            {yKeys.map((k, i) => <Area key={k} type="monotone" dataKey={k} stroke={COLORS[i % COLORS.length]} fill={COLORS[i % COLORS.length] + "33"} strokeWidth={2} />)}
          </AreaChart>
        </ResponsiveContainer>
      );
    }
    return (
      <ResponsiveContainer width="100%" height={280}>
        <BarChart data={data}><CartesianGrid strokeDasharray="3 3" stroke="#2a2d3a" />
          <XAxis dataKey={xKey} tick={{ fill: "#94a3b8", fontSize: 11 }} />
          <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} />
          <Tooltip contentStyle={{ background: "#1e2130", border: "1px solid #2a2d3a", borderRadius: 8 }} formatter={(v) => v.toLocaleString()} />
          <Legend />
          {yKeys.map((k, i) => <Bar key={k} dataKey={k} fill={COLORS[i % COLORS.length]} radius={[4, 4, 0, 0]} />)}
        </BarChart>
      </ResponsiveContainer>
    );
  }

  return (
    <div className="inline-chart">
      <div className="chart-type-switcher">
        {TYPES.map(t => (
          <button key={t} className={`ct-btn ${chartType === t ? "active" : ""}`} onClick={() => setChartType(t)}>
            {t === "bar" ? "📊" : t === "line" ? "📈" : t === "area" ? "🏔" : "🥧"} {t}
          </button>
        ))}
      </div>
      {renderChart()}
    </div>
  );
}
