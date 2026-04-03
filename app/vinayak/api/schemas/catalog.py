from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ProductCatalogUpsertRequest(BaseModel):
    sku: str = Field(min_length=1)
    name: str = Field(min_length=1)
    category: str = Field(default='general')
    description: str = Field(default='')
    attributes: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    is_active: bool = True


class ProductCatalogResponse(BaseModel):
    catalog_id: str | None = None
    sku: str
    name: str
    category: str
    description: str = ''
    attributes: dict[str, Any]
    tags: list[str]
    is_active: bool
    created_at: str | None = None
    updated_at: str | None = None


class ProductCatalogListResponse(BaseModel):
    total: int
    products: list[ProductCatalogResponse]
