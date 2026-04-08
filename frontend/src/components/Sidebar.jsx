export default function Sidebar({ view, setView }) {
  return (
    <div className="sidebar">
      <h2>Q</h2>
      <button onClick={() => setView("chat")}>💬</button>
      <button onClick={() => setView("viz")}>📊</button>
      <button onClick={() => setView("alerts")}>⚡</button>
    </div>
  );
}