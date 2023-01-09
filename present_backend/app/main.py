import os
import sys
import json
from typing import Final, List

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))


from kafka_distribute.consumer import consumer_optional
from fastapi import FastAPI
from typing import Optional



COIN_PRECENT_PRICE: Final[str] = "coin_price"
bootstrap_server: List[str] = ["kafka1:19091", "kafka2:29092", "kafka3:39093"]
price = consumer_optional(topic=COIN_PRECENT_PRICE, bootstrap_server=bootstrap_server)


app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Optional[str] = None):
    return {"item_id": item_id, "q": q}