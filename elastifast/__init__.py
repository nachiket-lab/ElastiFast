from fastapi import FastAPI
from .settings import load_settings

settings = load_settings()
app = FastAPI()