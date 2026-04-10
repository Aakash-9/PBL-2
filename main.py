# main.py — QueryMind NL2SQL Analytics Platform
import os
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import query, schema, visualize, alerts, session
import uvicorn

FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")

app = FastAPI(title="QueryMind NL2SQL API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL, "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(query.router,     prefix="/api", tags=["Query"])
app.include_router(schema.router,    prefix="/api", tags=["Schema"])
app.include_router(visualize.router, prefix="/api", tags=["Visualize"])
app.include_router(alerts.router,    prefix="/api", tags=["Alerts"])
app.include_router(session.router,   prefix="/api", tags=["Session"])

@app.get("/health")
def health():
    return {"status": "ok", "version": "2.0.0"}

if __name__ == "__main__":
    print("[QueryMind] Backend starting at http://localhost:8000")
    print(f"[QueryMind] Expecting frontend at {FRONTEND_URL}")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
