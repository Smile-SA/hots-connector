"""Unit tests for _clone_container_config against representative docker
inspect payloads (trimmed to the fields the function reads; the schema
matches real `docker inspect` output, hand-written since capturing a live
container wasn't available in this environment)."""

from __future__ import annotations

from hots_connector.adapters.docker.migration import _clone_container_config

_INSPECT_ATTRS = {
    "Config": {
        "Image": "nginx:1.25",
        "Cmd": ["nginx", "-g", "daemon off;"],
        "Entrypoint": None,
        "Env": ["FOO=bar", "BAZ=qux"],
        "Labels": {"app": "web", "hots.workload_id": "web-1"},
        "Hostname": "web-1",
        "User": "",
        "WorkingDir": "/usr/share/nginx/html",
        "ExposedPorts": {"80/tcp": {}},
    },
    "HostConfig": {
        "PortBindings": {"80/tcp": [{"HostIp": "", "HostPort": "8080"}]},
        "Binds": ["/data/web-1:/usr/share/nginx/html:ro"],
        "RestartPolicy": {"Name": "always", "MaximumRetryCount": 0},
    },
}


def test_clone_container_config_maps_ports_binds_and_restart_policy() -> None:
    cfg = _clone_container_config(_INSPECT_ATTRS)

    assert cfg["image"] == "nginx:1.25"
    assert cfg["command"] == ["nginx", "-g", "daemon off;"]
    assert cfg["environment"] == ["FOO=bar", "BAZ=qux"]
    assert cfg["labels"] == {"app": "web", "hots.workload_id": "web-1"}
    assert cfg["hostname"] == "web-1"
    assert cfg["user"] is None  # empty string normalized to None
    assert cfg["working_dir"] == "/usr/share/nginx/html"

    assert cfg["ports"] == {"80/tcp": ("", "8080")}
    assert cfg["volumes"] == {"/data/web-1": {"bind": "/usr/share/nginx/html", "mode": "ro"}}
    assert cfg["restart_policy"] == {"Name": "always", "MaximumRetryCount": 0}


def test_clone_container_config_no_restart_policy_maps_to_none() -> None:
    attrs = {
        "Config": {"Image": "redis:7"},
        "HostConfig": {"RestartPolicy": {"Name": "no"}},
    }

    cfg = _clone_container_config(attrs)

    assert cfg["restart_policy"] is None
    assert cfg["ports"] is None
    assert cfg["volumes"] is None


def test_clone_container_config_combines_entrypoint_and_cmd() -> None:
    attrs = {
        "Config": {
            "Image": "app:1",
            "Entrypoint": ["/entrypoint.sh"],
            "Cmd": ["serve", "--port", "8000"],
        },
        "HostConfig": {},
    }

    cfg = _clone_container_config(attrs)

    assert cfg["command"] == ["/entrypoint.sh", "serve", "--port", "8000"]


def test_clone_container_config_skips_unparseable_bind_spec() -> None:
    attrs = {
        "Config": {"Image": "app:1"},
        "HostConfig": {"Binds": ["not-a-valid-bind-spec"]},
    }

    cfg = _clone_container_config(attrs)

    assert cfg["volumes"] is None
