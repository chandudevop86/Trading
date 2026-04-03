from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from vinayak.api.dependencies.admin_auth import require_admin_session
from vinayak.api.schemas.catalog import ProductCatalogListResponse, ProductCatalogResponse, ProductCatalogUpsertRequest
from vinayak.catalog.service import ProductCatalogItem, ProductCatalogService


router = APIRouter(prefix='/catalog', tags=['catalog'], dependencies=[Depends(require_admin_session)])


@router.get('/products', response_model=ProductCatalogListResponse)
def list_products(
    category: str | None = Query(default=None),
    include_inactive: bool = Query(default=False),
) -> ProductCatalogListResponse:
    service = ProductCatalogService()
    try:
        products = [ProductCatalogResponse(**row) for row in service.list_products(category=category, include_inactive=include_inactive)]
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return ProductCatalogListResponse(total=len(products), products=products)


@router.post('/products', response_model=ProductCatalogResponse)
def upsert_product(request: ProductCatalogUpsertRequest) -> ProductCatalogResponse:
    service = ProductCatalogService()
    try:
        row = service.upsert_product(
            ProductCatalogItem(
                sku=request.sku,
                name=request.name,
                category=request.category,
                description=request.description,
                attributes=request.attributes,
                tags=request.tags,
                is_active=request.is_active,
            )
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return ProductCatalogResponse(**row)
