from math import log
from fastapi import FastAPI
from .settings import load_settings, logger

logger = logger
settings = load_settings()
app = FastAPI()