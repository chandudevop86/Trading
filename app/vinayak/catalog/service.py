from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from vinayak.core.config import get_settings

try:
    from pymongo import MongoClient
except Exception:  # pragma: no cover
    MongoClient = None  # type: ignore


@dataclass(slots=True)
class ProductCatalogItem:
    sku: str
    name: str
    category: str = 'general'
    description: str = ''
    attributes: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    is_active: bool = True


class ProductCatalogService:
    def __init__(self) -> None:
        self.settings = get_settings().mongo
        self._client = None

    def is_configured(self) -> bool:
        return bool(self.settings.url and MongoClient is not None)

    def _get_collection(self):
        if not self.is_configured():
            raise RuntimeError('MongoDB is not configured. Set MONGODB_URL and install pymongo.')
        if self._client is None:
            self._client = MongoClient(self.settings.url, serverSelectionTimeoutMS=1500)
        return self._client[self.settings.database][self.settings.product_collection]

    def readiness(self) -> dict[str, str]:
        if not self.is_configured():
            return {'status': 'disabled', 'engine': 'mongodb'}
        try:
            self._get_collection().database.client.admin.command('ping')
            return {'status': 'ok', 'engine': 'mongodb'}
        except Exception as exc:
            return {'status': 'error', 'engine': 'mongodb', 'detail': str(exc)}

    def upsert_product(self, item: ProductCatalogItem) -> dict[str, Any]:
        now = datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
        document = {
            'sku': item.sku,
            'name': item.name,
            'category': item.category,
            'description': item.description,
            'attributes': item.attributes,
            'tags': item.tags,
            'is_active': item.is_active,
            'updated_at': now,
        }
        collection = self._get_collection()
        existing = collection.find_one({'sku': item.sku})
        if existing is None:
            document['created_at'] = now
            document['catalog_id'] = uuid4().hex
            collection.insert_one(document)
        else:
            collection.update_one({'sku': item.sku}, {'$set': document})
            document['catalog_id'] = existing.get('catalog_id', '')
            document['created_at'] = existing.get('created_at', now)
        document.pop('_id', None)
        return document

    def list_products(self, *, category: str | None = None, include_inactive: bool = False) -> list[dict[str, Any]]:
        collection = self._get_collection()
        query: dict[str, Any] = {}
        if category:
            query['category'] = category
        if not include_inactive:
            query['is_active'] = True
        rows = list(collection.find(query, {'_id': 0}).sort('name', 1))
        return [dict(row) for row in rows]
