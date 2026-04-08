const API = "http://localhost:8000/api";

export async function askQuery(question, sessionId = "demo") {
  const res = await fetch(`${API}/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ question, session_id: sessionId }),
  });
  return res.json();
}

export async function getSchema() {
  const res = await fetch(`${API}/schema`);
  return res.json();
}

export async function visualizeData(selections, limit = 500) {
  const res = await fetch(`${API}/visualize`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ selections, limit }),
  });
  return res.json();
}

export async function getRecommendation(columns, sample_data) {
  const res = await fetch(`${API}/recommend`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ columns, sample_data }),
  });
  return res.json();
}
