from typing import Optional
from pydantic import BaseModel



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



class UserDelta(BaseModel):
    countryForm: str
    naprsForm: str
    resForm: str
    tnvedsForm: list
    yearsForm: str
    regionForm: str

