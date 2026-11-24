from __future__ import annotations

from fastapi import FastAPI

from ..actions.factory import build_action_backend
from ..config.settings import Settings
from ..observation.factory import build_observation_backend
from ..services.connector_service import ConnectorService
from ..transport.factory import build_transport
from ..version import __version__
from .routes import router


def create_app(*, settings: Settings) -> FastAPI:
    app = FastAPI(title="hots-connector", version=__version__)
    app.state.settings = settings
    app.state.service = ConnectorService(
        observation=build_observation_backend(settings),
        transport=build_transport(settings),
        actions=build_action_backend(settings),
        publish_interval_seconds=settings.publish_interval_seconds,
        moves_batch_size=settings.moves_batch_size,
    )
    app.include_router(router)
    return app