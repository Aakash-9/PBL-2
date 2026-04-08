import { useState, useRef, useEffect } from "react";
import { askQuery } from "../api/api";
import InlineChart from "./InlineChart";

const TABS = ["Insight", "SQL", "Reasoning", "Data"];
const CONF_COLOR = { HIGH: "#22c55e", MEDIUM: "#f59e0b", LOW: "#ef4444" };

export default function ChatView({ sessionId, messages, onUpdate }) {
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, loading]);

  async function handleSend() {
    if (!input.trim() || loading) return;
    const userMsg = { role: "user", content: input };
    const newMsgs = [...messages, userMsg];
    onUpdate(newMsgs);
    setInput("");
    setLoading(true);

    try {
      const data = await askQuery(input, sessionId);
      const assistantMsg = { role: "assistant", data, showViz: false, activeTab: "Insight" };
      onUpdate([...newMsgs, assistantMsg]);
    } catch (e) {
      onUpdate([...newMsgs, { role: "assistant", data: { insight: "Error: Could not reach backend.", sql: "", rows: [] }, showViz: false, activeTab: "Insight" }]);
    }
    setLoading(false);
  }

  function handleKey(e) {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); handleSend(); }
  }

  function toggleViz(idx) {
    const updated = messages.map((m, i) => i === idx ? { ...m, showViz: !m.showViz } : m);
    onUpdate(updated);
  }

  function setTab(idx, tab) {
    const updated = messages.map((m, i) => i === idx ? { ...m, activeTab: tab } : m);
    onUpdate(updated);
  }

  const hasData = (d) => d?.rows?.length > 0;
  const isComparison = (d) => d?.rows?.length > 1 && d?.rows?.[0] && Object.keys(d.rows[0]).length >= 2;

  return (
    <div className="chat-container">
      {/* Header */}
      <div className="chat-header">
        <div className="chat-header-title">
          <span className="logo-dot" />
          QueryMind
        </div>
        <div className="chat-header-sub">NL2SQL · RAG · Analytics</div>
      </div>

      {/* Messages */}
      <div className="messages">
        {messages.length === 0 && (
          <div className="welcome">
            <div className="welcome-icon">🧠</div>
            <h2>QueryMind AI Analytics</h2>
            <p>Ask any business question in plain English</p>
            <div className="suggestions">
              {["What is the GMV last month?", "Top 5 products by revenue", "Return rate by category", "Revenue trend last 30 days"].map(q => (
                <button key={q} className="suggestion-chip" onClick={() => { setInput(q); }}>
                  {q}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((msg, idx) => (
          <div key={idx} className={`message-row ${msg.role}`}>
            <div className="avatar">{msg.role === "user" ? "U" : "Q"}</div>
            <div className="message-content">
              {msg.role === "user" ? (
                <div className="user-bubble">{msg.content}</div>
              ) : (
                <div className="assistant-bubble">
                  {/* Clarification response — no tabs, just the message */}
                  {msg.data?.clarification_needed ? (
                    <div className="clarification-msg">
                      <span className="clarification-icon">💬</span>
                      <p>{msg.data.note}</p>
                    </div>
                  ) : (
                  <>
                  {/* Tab bar */}
                  <div className="msg-tabs">
                    {TABS.map(t => (
                      <button
                        key={t}
                        className={`msg-tab ${msg.activeTab === t ? "active" : ""}`}
                        onClick={() => setTab(idx, t)}
                      >{t}</button>
                    ))}
                    {msg.data?.confidence && (
                      <span className="conf-badge" style={{ background: CONF_COLOR[msg.data.confidence] }}>
                        {msg.data.confidence}
                      </span>
                    )}
                  </div>

                  {/* Note banner — shown when there's a warning or no data */}
                  {msg.data?.note && (
                    <div className="note-banner">
                      ⚠️ {msg.data.note}
                    </div>
                  )}

                  {/* Tab content */}
                  <div className="msg-body">
                    {msg.activeTab === "Insight" && (
                      <p className="insight-text">
                        {msg.data?.insight || (msg.data?.note ? msg.data.note : "No insight generated.")}
                      </p>
                    )}
                    {msg.activeTab === "SQL" && (
                      <div>
                        <pre className="sql-block">{msg.data?.sql || "No SQL generated."}</pre>
                        {msg.data?.sql && (
                          <button className="copy-btn" onClick={() => navigator.clipboard.writeText(msg.data?.sql)}>Copy SQL</button>
                        )}
                      </div>
                    )}
                    {msg.activeTab === "Reasoning" && (
                      <div>
                        <p className="insight-text">{msg.data?.reasoning || "No reasoning."}</p>
                        {msg.data?.chunks_used?.length > 0 && (
                          <div className="chunks-wrap">
                            <div className="chunks-label">RAG context ({msg.data.chunks_used.length} chunks)</div>
                            {msg.data.chunks_used.map((c, i) => (
                              <div key={i} className="chunk-row">
                                <span className="chunk-src">{c.source}</span> {c.text?.slice(0, 90)}...
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    )}
                    {msg.activeTab === "Data" && (
                      <div>
                        <div className="row-count">{msg.data?.row_count ?? 0} rows</div>
                        {hasData(msg.data) ? (
                          <div className="table-wrap">
                            <table className="data-table">
                              <thead><tr>{Object.keys(msg.data.rows[0]).map(k => <th key={k}>{k}</th>)}</tr></thead>
                              <tbody>{msg.data.rows.slice(0, 50).map((r, i) => (
                                <tr key={i}>{Object.values(r).map((v, j) => <td key={j}>{String(v ?? "")}</td>)}</tr>
                              ))}</tbody>
                            </table>
                          </div>
                        ) : <p className="dim">No data returned.</p>}
                      </div>
                    )}
                  </div>

                  {/* Visualize button */}
                  {isComparison(msg.data) && (
                    <button className="viz-toggle-btn" onClick={() => toggleViz(idx)}>
                      {msg.showViz ? "▲ Hide Chart" : "📊 Visualize"}
                    </button>
                  )}

                  {/* Inline chart */}
                  {msg.showViz && isComparison(msg.data) && (
                    <InlineChart rows={msg.data.rows} />
                  )}
                  </>
                  )}
                </div>
              )}
            </div>
          </div>
        ))}

        {loading && (
          <div className="message-row assistant">
            <div className="avatar">Q</div>
            <div className="message-content">
              <div className="assistant-bubble">
                <div className="typing"><span /><span /><span /></div>
              </div>
            </div>
          </div>
        )}
        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div className="input-area">
        <div className="input-box">
          <textarea
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKey}
            placeholder="Ask a business question..."
            rows={1}
          />
          <button className="send-btn" onClick={handleSend} disabled={loading || !input.trim()}>
            ↑
          </button>
        </div>
        <div className="input-hint">Press Enter to send · Shift+Enter for new line</div>
      </div>
    </div>
  );
}
