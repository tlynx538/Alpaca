from pydantic import BaseModel
from typing import Union, List

class CodeRequest(BaseModel):
    code: str

class CodeCompleteRequest(BaseModel):
    msg_id: str

class PackageInstallRequest(BaseModel):
    packages: Union[str, List[str]]
    upgrade: bool = False
    timeout: int = 300