import numpy as np
from typing import Any, Tuple, List, Dict
from backend.coin_price.schema.schema import utc_time


def test_straming_preprocessing(*data: Tuple[Any]) -> Dict:
  roww: List = [d for d in data]  
  value: List[Tuple] = list(zip(*roww))
  average: Any = np.mean(value, axis=1)
  data: Dict = {
      "average": {
        "name": "BTC",
        "timestamp": utc_time(),
        "data": {
            "opening_price": average[0],
            "closing_price": average[1],
            "max_price": average[2],
            "min_price": average[3]
        }
    }
  }
  return data
