from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from ufcscraper.base import BaseFileHandler

if TYPE_CHECKING: # pragma: no cover
    from typing import Dict

logger = logging.getLogger(__name__)

class CatchWeights(BaseFileHandler):
    dtypes: Dict[str, type | pd.core.arrays.integer.Int64Dtype] = {
        "fight_id": str,
        "weight": pd.Int64Dtype(),
    }

    sort_fields = ["fight_id", "weight"]
    data = pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in dtypes.items()})
    filename = "catch_weights.csv"