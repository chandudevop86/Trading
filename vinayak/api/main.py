from fastapi import FastAPI

from vinayak.api.routes.dashboard import router as dashboard_router
from vinayak.api.routes.executions import router as executions_router
from vinayak.api.routes.health import router as health_router
from vinayak.api.routes.reviewed_trades import router as reviewed_trades_router
from vinayak.api.routes.signals import router as signals_router
from vinayak.api.routes.strategies import router as strategies_router
from vinayak.web.app.main import router as web_router


app = FastAPI(title='Vinayak Trading Platform', version='0.1.0')
app.include_router(health_router)
app.include_router(signals_router)
app.include_router(reviewed_trades_router)
app.include_router(executions_router)
app.include_router(dashboard_router)
app.include_router(strategies_router)
app.include_router(web_router)

