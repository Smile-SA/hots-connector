"""Unit tests for the CLI's --version flag and its dotenv lookup
(load_settings / HOTSCONNECTOR_ENV_FILE)."""

from __future__ import annotations

from importlib.metadata import version
from pathlib import Path

import pytest

from hots_connector import __version__
from hots_connector.cli import main
from hots_connector.config.settings import load_settings


def test_version_comes_from_package_metadata() -> None:
    assert __version__ == version("hots-connector")
    assert __version__ != "0.0.0"


def test_version_flag_prints_version_and_exits_zero(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--version"])

    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == f"hots-connector {__version__}"


def test_load_settings_reads_dotenv_in_working_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / ".env").write_text("HOTSCONNECTOR_LOG_LEVEL=DEBUG\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("HOTSCONNECTOR_ENV_FILE", raising=False)
    monkeypatch.delenv("HOTSCONNECTOR_LOG_LEVEL", raising=False)

    assert load_settings().log_level == "DEBUG"


def test_load_settings_reads_the_file_named_by_env_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "custom.env").write_text("HOTSCONNECTOR_LOG_LEVEL=WARNING\n")
    monkeypatch.setenv("HOTSCONNECTOR_ENV_FILE", str(tmp_path / "custom.env"))
    monkeypatch.delenv("HOTSCONNECTOR_LOG_LEVEL", raising=False)

    assert load_settings().log_level == "WARNING"


def test_empty_env_file_ignores_a_stray_dotenv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / ".env").write_text("HOTSCONNECTOR_LOG_LEVEL=DEBUG\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOTSCONNECTOR_ENV_FILE", "")
    monkeypatch.delenv("HOTSCONNECTOR_LOG_LEVEL", raising=False)

    assert load_settings().log_level == "INFO"
