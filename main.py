# main.py — QueryMind NL2SQL Analytics Platform
import os
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from routers import query, schema, visualize, alerts, session
import uvicorn

app = FastAPI(title="QueryMind NL2SQL API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(query.router,     prefix="/api", tags=["Query"])
app.include_router(schema.router,    prefix="/api", tags=["Schema"])
app.include_router(visualize.router, prefix="/api", tags=["Visualize"])
app.include_router(alerts.router,    prefix="/api", tags=["Alerts"])
app.include_router(session.router,   prefix="/api", tags=["Session"])

# Serve frontend
@app.get("/")
def serve_frontend():
    return FileResponse("frontend/index.html")

@app.get("/health")
def health():
    return {"status": "ok", "version": "2.0.0"}

if __name__ == "__main__":
    import subprocess, threading, sys

    frontend_dir = os.path.join(os.path.dirname(__file__), "frontend")
    node_path = r"C:\Program Files\nodejs"
    npm = os.path.join(node_path, "npm.cmd")
    env = {**os.environ, "PATH": node_path + os.pathsep + os.environ.get("PATH", "")}

    def run_vite():
        subprocess.run([npm, "run", "dev"], cwd=frontend_dir, env=env)

    vite_thread = threading.Thread(target=run_vite, daemon=True)
    vite_thread.start()
    print("[QueryMind] Frontend starting at http://localhost:5173")
    print("[QueryMind] Backend starting at http://localhost:8000")

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
