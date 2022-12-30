import os
import sys
import json
from typing import Final, List

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))


from kafka_distribute.consumer import consumer_optional


COIN_PRECENT_PRICE: Final[str] = "coin_price"
bootstrap_server: List[str] = ["kafka1:19091", "kafka2:29092", "kafka3:39093"]
price = consumer_optional(topic=COIN_PRECENT_PRICE, bootstrap_server=bootstrap_server)


for index in (json.loads(i.value.decode()) for i in price):
    print(index)

    
