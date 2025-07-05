from fastapi import FastAPI
from internal.router import router
from fastapi.middleware.cors import CORSMiddleware


origins = [
    "http://localhost:3000",
    "*",
    "http://192.168.0.131:3000"

]

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def home():
    return {"message": "This is Alpaca API. Use the endpoints to interact with the Jupyter kernel."}

app.include_router(router, prefix="/api", tags=["kernel"])
