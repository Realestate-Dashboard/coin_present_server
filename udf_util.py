import sys
from pathlib import Path

# 현재 파일의 경로
file_path = Path(__file__).resolve()

# 상위 경로 backend
parent_path = file_path.parent

# 상위 경로 backed_pre -> sys 경로 추가
grandparent_path = parent_path.parent
sys.path.append(str(parent_path))
sys.path.append(str(grandparent_path))

import numpy as np
from typing import Any, Tuple, List, Dict
from backend.coin_price.schema.schema import utc_time


def test_straming_preprocessing(*data: Tuple[Any]) -> Dict:
  roww: List = [d for d in data]  
  value: List[Tuple] = list(zip(*roww))
  average: Any = np.mean(value, axis=1).tolist()
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
