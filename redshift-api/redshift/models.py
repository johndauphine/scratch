from typing import Optional, List
from pydantic import BaseModel
from enum import Enum


class NodeType(str,Enum):
    ra3_xlplus = "ra3.xlplus"
    dc2_large = "dc2.large"


class ClusterResizeRequest(BaseModel):
    node_type: str
    node_count: int

