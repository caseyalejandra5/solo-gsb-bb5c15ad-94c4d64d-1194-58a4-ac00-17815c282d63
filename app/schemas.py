"""请求/响应模型。"""
from __future__ import annotations

from pydantic import BaseModel, Field


class BucketItem(BaseModel):
    key: str = Field(min_length=1, max_length=128)
    amount: int = Field(gt=0, le=2**31 - 1)


class ConsumeRequest(BaseModel):
    request_id: str = Field(min_length=1, max_length=128)
    buckets: list[BucketItem] = Field(min_length=1, max_length=100)


class ReleaseRequest(BaseModel):
    release_id: str = Field(min_length=1, max_length=128)
    request_id: str = Field(min_length=1, max_length=128)


class BucketConfig(BaseModel):
    limit: int = Field(gt=0, le=2**31 - 1)
    window_seconds: float = Field(gt=0, le=365 * 24 * 3600)
