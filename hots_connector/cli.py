from __future__ import annotations

import argparse
import logging

from .actions.factory import build_action_backend
from .api.app import create_app
from .config.settings import Settings, load_settings
from .observation.factory import build_observation_backend
from .services.connector_service import ConnectorService
from .transport.factory import build_transport
from .version import __version__


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _build_service(settings: Settings) -> ConnectorService:
    return ConnectorService(
        observation=build_observation_backend(settings),
        transport=build_transport(settings),
        actions=build_action_backend(settings),
        publish_interval_seconds=settings.publish_interval_seconds,
        moves_batch_size=settings.moves_batch_size,
    )


def cmd_api(args: argparse.Namespace) -> int:
    import uvicorn

    settings = load_settings()
    _configure_logging(settings.log_level)

    app = create_app(settings=settings)
    uvicorn.run(app, host=args.host, port=args.port, log_level=settings.log_level.lower())
    return 0


def cmd_run(_: argparse.Namespace) -> int:
    settings = load_settings()
    _configure_logging(settings.log_level)
    _build_service(settings).run_forever()
    return 0


def cmd_check(_: argparse.Namespace) -> int:
    settings = load_settings()
    _configure_logging(settings.log_level)
    report = _build_service(settings).validate()
    print(report)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="hots-connector")
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = p.add_subparsers(dest="cmd", required=True)

    api = sub.add_parser("api", help="Run HTTP API")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8000)
    api.set_defaults(func=cmd_api)

    run = sub.add_parser("run", help="Run connector loop (no HTTP)")
    run.set_defaults(func=cmd_run)

    chk = sub.add_parser("check", help="Check connectivity to dependencies")
    chk.set_defaults(func=cmd_check)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))