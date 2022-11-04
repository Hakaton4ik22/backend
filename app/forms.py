from typing import Optional
from pydantic import BaseModel


class StreamForm(BaseModel):
    title: str
    topic: str
    status: Optional[str] = None
    description: Optional[str] = None


class StreamUpdateForm(BaseModel):
    stream_id: int
    status: str


class UserDataUpdateForm(BaseModel):
    napr: str
    nastranapr: str
    tnved_description: str
    kol: str
    stoim: str




class UserLoginForm(BaseModel):
    email: str
    password: str
