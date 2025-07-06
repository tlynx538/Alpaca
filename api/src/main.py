from fastapi import FastAPI
from internal.router import router
from fastapi.middleware.cors import CORSMiddleware
from internal.middleware import debug_middleware

origins = [
    "*",
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

app.middleware('http')(debug_middleware)
app.include_router(router, prefix="/api", tags=["kernel"])
