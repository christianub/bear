from pydantic import BaseModel

API = "instantgmp"
BUCKET = "api-ingestion-bucket"

class Auth(BaseModel):
    api_user: str
    api_pass: str
    base_url: str

class FlowParams(BaseModel):
    """Whatever the shape of the flow params is should be defined here."""

    f_number: str
    auth: Auth