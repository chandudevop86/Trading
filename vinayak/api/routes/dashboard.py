from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from vinayak.api.dependencies.admin_auth import require_admin_session
from vinayak.api.dependencies.db import get_db
from vinayak.api.schemas.signal import DashboardSummaryResponse
from vinayak.api.services.dashboard_summary import DashboardSummaryService


router = APIRouter(prefix='/dashboard', tags=['dashboard'], dependencies=[Depends(require_admin_session)])


@router.get('/summary', response_model=DashboardSummaryResponse)
def get_dashboard_summary(db: Session = Depends(get_db)) -> DashboardSummaryResponse:
    service = DashboardSummaryService(db)
    return DashboardSummaryResponse(**service.build_summary())
