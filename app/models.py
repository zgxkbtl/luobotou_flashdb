from typing import Union
from pydantic import BaseModel


class DriverMeta(BaseModel):
    name: str
    model: str
    vendor: str
    capacity: int   # storage GB
    interface: str  # interfaceType typec sata usb
    date: str       # date of release
    description: Union[str, None] = None
    price: Union[int, None] = None  # price in RMB

class ControllerMeta(BaseModel):
    model: str
    vendor: str    
    description: str

query = (
    "1123 "
    "123  "*3
)
print(query)