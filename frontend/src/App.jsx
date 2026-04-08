import React, { useState } from "react";
import ChatView from "./components/ChatView";

export default function App() {
  const [sessions, setSessions] = useState([
    { id: "default", title: "New Chat", messages: [] }
  ]);
  const [activeId, setActiveId] = useState("default");
  const [collapsed, setCollapsed] = useState(false);

  function newChat() {
    const id = Date.now().toString();
    setSessions(s => [...s, { id, title: "New Chat", messages: [] }]);
    setActiveId(id);
  }

  function updateSession(id, messages) {
    setSessions(s => s.map(sess => {
      if (sess.id !== id) return sess;
      const firstQ = messages.find(m => m.role === "user");
      const title = firstQ ? firstQ.content.slice(0, 36) + (firstQ.content.length > 36 ? "..." : "") : "New Chat";
      return { ...sess, messages, title };
    }));
  }

  const active = sessions.find(s => s.id === activeId);

  return (
    <div className="app">
      {/* Sidebar */}
      <div className={`sidebar ${collapsed ? "collapsed" : ""}`}>
        <div className="sidebar-top">
          <button className="icon-btn" onClick={() => setCollapsed(c => !c)} title="Toggle sidebar">
            {collapsed ? "→" : "←"}
          </button>
          {!collapsed && (
            <button className="new-chat-btn" onClick={newChat}>+ New Chat</button>
          )}
        </div>
        {!collapsed && (
          <div className="chat-history">
            {[...sessions].reverse().map(s => (
              <div
                key={s.id}
                className={`history-item ${s.id === activeId ? "active" : ""}`}
                onClick={() => setActiveId(s.id)}
              >
                <span className="history-icon">💬</span>
                <span className="history-title">{s.title}</span>
              </div>
            ))}
          </div>
        )}
        {!collapsed && (
          <div className="sidebar-footer">
            <div className="model-badge">⚡ LLaMA 3.3 · 70B</div>
          </div>
        )}
      </div>

      {/* Main */}
      <div className="main">
        <ChatView
          key={activeId}
          sessionId={activeId}
          messages={active?.messages || []}
          onUpdate={(msgs) => updateSession(activeId, msgs)}
        />
      </div>
    </div>
  );
}
