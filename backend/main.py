import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.routers.dashboard_router import router as dashboard_router

app = FastAPI(
    title="EMEA Dashboard API",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_no_cache_header(request, call_next):
    response = await call_next(request)
    if request.url.path == "/" or request.url.path.endswith(".html"):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response

from fastapi.staticfiles import StaticFiles


@app.get("/health")
def health_check():
    return {"status": "healthy"}

app.include_router(dashboard_router, prefix="/api/v1")
app.include_router(dashboard_router, prefix="/api")

# Mount frontend directory for static hosting
frontend_path = os.path.join(os.path.dirname(__file__), "../frontend")
if os.path.isdir(frontend_path):
    app.mount("/", StaticFiles(directory=frontend_path, html=True), name="frontend")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
