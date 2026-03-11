from fastapi import FastAPI
from app.routes.health import router as health_router
from app.routes.exports import router as exports_router
app = FastAPI()
app.include_router(health_router)
app.include_router(exports_router)
