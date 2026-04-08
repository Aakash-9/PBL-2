import { useState, useEffect } from "react";
import {
  BarChart, Bar, LineChart, Line, AreaChart, Area,
  ScatterChart, Scatter, PieChart, Pie, Cell,
  XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid
} from "recharts";
import { getSchema, visualizeData, getRecommendation } from "../api/api";

const COLORS = ["#6366f1", "#22c55e", "#f59e0b", "#ef4444", "#06b6d4", "#a855f7", "#f97316"];
const CHART_TYPES = ["bar", "line", "area", "scatter", "pie", "table"];

export default function VizDashboard() {
  const [schema, setSchema] = useState([]);
  const [expandedTable, setExpandedTable] = useState(null);
  const [selected, setSelected] = useState([]); // [{table, column, dtype}]
  const [chartType, setChartType] = useState("bar");
  const [recommendation, setRecommendation] = useState(null);
  const [vizData, setVizData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    getSchema().then(s => setSchema(s.tables || []));
  }, []);

  function toggleColumn(table, col) {
    const key = `${table}__${col.name}`;
    const exists = selected.find(s => s.key === key);
    if (exists) {
      setSelected(selected.filter(s => s.key !== key));
    } else {
      setSelected([...selected, { key, table, column: col.name, dtype: col.dtype }]);
    }
  }

  function isSelected(table, colName) {
    return !!selected.find(s => s.key === `${table}__${colName}`);
  }

  async function handleVisualize() {
    if (selected.length === 0) return;
    setLoading(true);
    setError(null);

    // Group selections by table
    const grouped = {};
    selected.forEach(s => {
      if (!grouped[s.table]) grouped[s.table] = [];
      grouped[s.table].push(s.column);
    });
    const selections = Object.entries(grouped).map(([table, columns]) => ({ table, columns }));

    const result = await visualizeData(selections);
    if (!result.success) {
      setError(result.error || "Failed to fetch data");
      setLoading(false);
      return;
    }

    setVizData(result.rows || []);

    // Get LLM recommendation
    const cols = selected.map(s => ({ name: s.key, dtype: s.dtype }));
    const rec = await getRecommendation(cols, result.rows.slice(0, 20));
    setRecommendation(rec);
    if (rec?.best?.type && CHART_TYPES.includes(rec.best.type)) {
      setChartType(rec.best.type);
    }
    setLoading(false);
  }

  function clearAll() {
    setSelected([]);
    setVizData([]);
    setRecommendation(null);
    setError(null);
  }

  const dataKeys = vizData.length > 0 ? Object.keys(vizData[0]) : [];
  const xKey = dataKeys[0] || "";
  const yKeys = dataKeys.slice(1);

  function renderChart() {
    if (vizData.length === 0) return null;

    if (chartType === "table") {
      return (
        <div className="data-table-wrap">
          <table className="data-table">
            <thead><tr>{dataKeys.map(k => <th key={k}>{k}</th>)}</tr></thead>
            <tbody>
              {vizData.slice(0, 100).map((row, i) => (
                <tr key={i}>{dataKeys.map(k => <td key={k}>{String(row[k] ?? "")}</td>)}</tr>
              ))}
            </tbody>
          </table>
        </div>
      );
    }

    if (chartType === "pie") {
      const pieData = vizData.slice(0, 10).map(r => ({
        name: String(r[xKey] ?? ""),
        value: parseFloat(r[yKeys[0]]) || 0
      }));
      return (
        <ResponsiveContainer width="100%" height={350}>
          <PieChart>
            <Pie data={pieData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={130} label>
              {pieData.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
            </Pie>
            <Tooltip /><Legend />
          </PieChart>
        </ResponsiveContainer>
      );
    }

    if (chartType === "scatter") {
      return (
        <ResponsiveContainer width="100%" height={350}>
          <ScatterChart><CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey={xKey} /><YAxis dataKey={yKeys[0]} />
            <Tooltip />
            <Scatter data={vizData} fill="#6366f1" />
          </ScatterChart>
        </ResponsiveContainer>
      );
    }

    if (chartType === "line") {
      return (
        <ResponsiveContainer width="100%" height={350}>
          <LineChart data={vizData}><CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey={xKey} /><YAxis /><Tooltip /><Legend />
            {yKeys.map((k, i) => <Line key={k} type="monotone" dataKey={k} stroke={COLORS[i % COLORS.length]} dot={false} />)}
          </LineChart>
        </ResponsiveContainer>
      );
    }

    if (chartType === "area") {
      return (
        <ResponsiveContainer width="100%" height={350}>
          <AreaChart data={vizData}><CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey={xKey} /><YAxis /><Tooltip /><Legend />
            {yKeys.map((k, i) => <Area key={k} type="monotone" dataKey={k} stroke={COLORS[i % COLORS.length]} fill={COLORS[i % COLORS.length] + "44"} />)}
          </AreaChart>
        </ResponsiveContainer>
      );
    }

    // default bar
    return (
      <ResponsiveContainer width="100%" height={350}>
        <BarChart data={vizData}><CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey={xKey} /><YAxis /><Tooltip /><Legend />
          {yKeys.map((k, i) => <Bar key={k} dataKey={k} fill={COLORS[i % COLORS.length]} />)}
        </BarChart>
      </ResponsiveContainer>
    );
  }

  return (
    <div className="viz-layout">
      {/* LEFT: Schema picker */}
      <div className="viz-sidebar">
        <div className="viz-sidebar-title">📊 Select Columns</div>
        {schema.map(table => (
          <div key={table.name} className="schema-table">
            <div
              className="schema-table-header"
              onClick={() => setExpandedTable(expandedTable === table.name ? null : table.name)}
            >
              <span>{expandedTable === table.name ? "▾" : "▸"} {table.name}</span>
              <span className="col-count">{table.columns.length} cols</span>
            </div>
            {expandedTable === table.name && (
              <div className="schema-columns">
                {table.columns.map(col => (
                  <label key={col.name} className="col-item">
                    <input
                      type="checkbox"
                      checked={isSelected(table.name, col.name)}
                      onChange={() => toggleColumn(table.name, col)}
                    />
                    <span className="col-name">{col.name}</span>
                    <span className="col-dtype">{col.dtype}</span>
                  </label>
                ))}
              </div>
            )}
          </div>
        ))}
      </div>

      {/* RIGHT: Chart area */}
      <div className="viz-main">
        {/* Top bar */}
        <div className="viz-topbar">
          <div className="selected-pills">
            {selected.length === 0
              ? <span className="hint">Select columns from the left to visualize</span>
              : selected.map(s => (
                <span key={s.key} className="pill">
                  {s.key}
                  <button onClick={() => toggleColumn(s.table, { name: s.column })}>×</button>
                </span>
              ))
            }
          </div>
          <div className="viz-actions">
            {recommendation?.best && (
              <div className="rec-badge">
                {recommendation.best.icon} Best: <strong>{recommendation.best.type}</strong>
                <span className="rec-reason"> — {recommendation.best.reason}</span>
              </div>
            )}
          </div>
        </div>

        {/* Chart type selector */}
        <div className="chart-type-bar">
          {CHART_TYPES.map(t => (
            <button
              key={t}
              className={`chart-type-btn ${chartType === t ? "active" : ""}`}
              onClick={() => setChartType(t)}
            >
              {t === "bar" ? "📊" : t === "line" ? "📈" : t === "area" ? "🏔" : t === "scatter" ? "🔵" : t === "pie" ? "🥧" : "📋"} {t}
            </button>
          ))}
          <button className="viz-btn" onClick={handleVisualize} disabled={loading || selected.length === 0}>
            {loading ? "Loading..." : "▶ Visualize"}
          </button>
          {selected.length > 0 && <button className="clear-btn" onClick={clearAll}>✕ Clear</button>}
        </div>

        {/* LLM recommendations list */}
        {recommendation?.rule_based?.recommendations?.length > 0 && (
          <div className="rec-list">
            <span className="rec-list-title">AI suggestions:</span>
            {recommendation.rule_based.recommendations.slice(0, 4).map((r, i) => (
              <button
                key={i}
                className={`rec-chip ${chartType === r.type ? "active" : ""}`}
                onClick={() => setChartType(r.type)}
              >
                {r.icon} {r.type} <span className="rec-score">{r.score}</span>
              </button>
            ))}
          </div>
        )}

        {error && <div className="error-box">⚠ {error}</div>}

        {/* Chart */}
        <div className="chart-area">
          {vizData.length > 0 ? renderChart() : (
            <div className="chart-placeholder">
              Select columns and click Visualize
            </div>
          )}
        </div>

        {vizData.length > 0 && (
          <div className="viz-meta">{vizData.length} rows · {dataKeys.length} columns</div>
        )}
      </div>
    </div>
  );
}
