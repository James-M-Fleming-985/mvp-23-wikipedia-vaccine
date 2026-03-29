"""Auto-generated FastAPI wrapper for layer_mvp_0023."""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from src.layer_mvp_0023 import get_vaccine_search_trends, calculate_granger_causality, generate_market_metrics, process_trend_correlation, prepare_data_for_analysis

app = FastAPI(
    title="Layer Mvp 0023",
    description="Auto-generated MVP API",
    version="1.0.0",
)

@app.get("/")
def root():
    return {"service": "layer_mvp_0023", "status": "running", "version": "1.0.0"}

@app.get("/health")
def health():
    return {"status": "healthy"}

