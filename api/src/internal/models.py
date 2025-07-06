from pydantic import BaseModel

class CodeRequest(BaseModel):
    code: str

class CodeCompleteRequest(BaseModel):
    msg_id: str