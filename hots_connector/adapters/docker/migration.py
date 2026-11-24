from __future__ import annotations

import io
import logging
import time
from dataclasses import dataclass
from typing import Any

from ...config.settings import Settings
from ...domain.models import PlacementMove
from .registry import DockerHostRegistry

log = logging.getLogger(__name__)

# Statuses that mean the target will never become Ready on its own; waiting
# out the full timeout in these states would just delay reporting a failure.
_TERMINAL_BAD_STATUSES = {"exited", "dead", "removing"}


def _first_repo_tag(image_attrs: dict[str, Any]) -> str | None:
    tags = image_attrs.get("RepoTags") or []
    return tags[0] if tags else None


def _map_port_bindings(host_config: dict[str, Any]) -> dict[str, Any] | None:
    """HostConfig.PortBindings -> the `ports=` shape `containers.run()` expects.

    Raw shape: {"80/tcp": [{"HostIp": "", "HostPort": "8080"}], ...}
    Target shape: {"80/tcp": ("", "8080")} (or a list of tuples for multiple
    host bindings on the same container port).
    """
    raw = host_config.get("PortBindings") or {}
    if not raw:
        return None

    mapped: dict[str, Any] = {}
    for container_port, host_bindings in raw.items():
        if not host_bindings:
            continue
        entries = [
            (b.get("HostIp") or "", b.get("HostPort")) for b in host_bindings if b.get("HostPort")
        ]
        if not entries:
            continue
        mapped[container_port] = entries[0] if len(entries) == 1 else entries
    return mapped or None


def _map_binds(host_config: dict[str, Any]) -> dict[str, Any] | None:
    """HostConfig.Binds (["/host:/container:ro", ...]) -> `volumes=` dict shape."""
    binds = host_config.get("Binds") or []
    if not binds:
        return None

    mapped: dict[str, Any] = {}
    for bind in binds:
        parts = bind.split(":")
        if len(parts) < 2:
            log.warning("Skipping unparseable bind mount spec: %r", bind)
            continue
        host_path, container_path = parts[0], parts[1]
        mode = parts[2] if len(parts) > 2 else "rw"
        mapped[host_path] = {"bind": container_path, "mode": mode}
    return mapped or None


def _map_restart_policy(host_config: dict[str, Any]) -> dict[str, Any] | None:
    """HostConfig.RestartPolicy, passed through as-is when a policy is actually set."""
    policy = host_config.get("RestartPolicy") or {}
    name = policy.get("Name")
    if not name or name == "no":
        return None
    return policy


def _clone_container_config(container_attrs: dict[str, Any]) -> dict[str, Any]:
    """
    Best-effort extraction of container creation config from docker inspect attrs.

    Docker SDK expects create_container/run style fields:
    - image, name, command, environment, labels, hostname, user, working_dir
    - ports, binds and restart policy are mapped from HostConfig below;
      everything else in HostConfig (resource limits, network mode, capabilities,
      security options, ...) is NOT migrated -- see README "migration semantics".
    """
    cfg = container_attrs.get("Config", {}) or {}
    host_cfg = container_attrs.get("HostConfig", {}) or {}

    env = cfg.get("Env") or []
    labels = cfg.get("Labels") or {}

    # Command: Docker inspect provides Cmd list and Entrypoint
    entrypoint = cfg.get("Entrypoint")
    cmd = cfg.get("Cmd")

    command = None
    if entrypoint and cmd:
        command = list(entrypoint) + list(cmd)
    elif entrypoint:
        command = list(entrypoint)
    elif cmd:
        command = list(cmd)

    return {
        "image": cfg.get("Image"),
        "command": command,
        "environment": env,
        "labels": labels,
        "hostname": cfg.get("Hostname"),
        "user": cfg.get("User") or None,
        "working_dir": cfg.get("WorkingDir") or None,
        "ports": _map_port_bindings(host_cfg),
        "volumes": _map_binds(host_cfg),
        "restart_policy": _map_restart_policy(host_cfg),
        # Kept around for diagnostics / future extensions (network mode,
        # resource limits, capabilities, ...) that are still not migrated.
        "host_config_raw": host_cfg,
    }


@dataclass
class DockerMigrator:
    settings: Settings
    registry: DockerHostRegistry

    @classmethod
    def from_settings(cls, settings: Settings) -> DockerMigrator:
        return cls(settings=settings, registry=DockerHostRegistry(settings))

    def ping(self) -> bool:
        # ping default host (or any configured host if present)
        hosts = self.settings.docker_hosts()
        if hosts:
            # ping one arbitrarily
            hid = next(iter(hosts.keys()))
            return self.registry.ping(hid)
        # else ping default local
        try:
            self.registry.get("default").ping()
            return True
        except Exception:
            log.exception("Docker ping failed")
            return False

    def apply(self, move: PlacementMove) -> None:
        if self.settings.migration_strategy != "recreate":
            raise ValueError(f"Unsupported migration_strategy={self.settings.migration_strategy!r}")

        if move.source_host is None:
            # "recreate" is a migration strategy: it needs a known source to copy
            # config/image from and to stop afterwards. An initial placement
            # (old_host=null in the move contract) has no source container to
            # clone -- that's a "create fresh" operation this backend doesn't
            # implement yet. Fail clearly instead of crashing inside registry.get(None).
            raise ValueError(
                f"Cannot recreate-migrate {move.workload_id!r}: no source_host "
                "(this looks like an initial placement, which the docker "
                "'recreate' strategy does not support)"
            )

        self._recreate_on_target(move)

    def _recreate_on_target(self, move: PlacementMove) -> None:
        # `apply()` already rejects a None source_host before calling this;
        # asserted again here so the type checker can narrow it too.
        assert move.source_host is not None
        src = self.registry.get(move.source_host)
        dst = self.registry.get(move.target_host)

        # 1) Find container on source
        container = self._get_container(src, move.workload_id)

        # 2) Inspect container config + image
        attrs = container.attrs
        image_id = attrs.get("Image")
        image_ref = None
        try:
            img = src.images.get(image_id)
            image_ref = _first_repo_tag(img.attrs) or image_id
        except Exception:
            image_ref = image_id

        if not image_ref:
            raise RuntimeError(f"Could not resolve image for container {move.workload_id}")

        cfg = _clone_container_config(attrs)

        target_name = move.workload_id if self.settings.migration_keep_name else None

        if self.settings.migration_dry_run:
            log.info(
                "[dry-run] would migrate %r: %s -> %s (image=%s, ports=%s, volumes=%s, "
                "restart_policy=%s, remove_source=%s)",
                move.workload_id,
                move.source_host,
                move.target_host,
                image_ref,
                cfg["ports"],
                cfg["volumes"],
                cfg["restart_policy"],
                self.settings.migration_remove_source,
            )
            return

        # 3) Ensure image exists on target (pull if tag, else save/load)
        self._ensure_image_on_target(src=src, dst=dst, image_ref=image_ref, image_id=image_id)

        # 4) Create container on target
        if target_name:
            from docker.errors import NotFound

            # If already exists on target, fail early to avoid overwriting
            try:
                dst.containers.get(target_name)
                raise RuntimeError(
                    f"Target already has a container named "
                    f"{target_name!r} on host {move.target_host!r}"
                )
            except NotFound:
                pass

        log.info(
            "Creating container %r on %r using image %r", target_name, move.target_host, image_ref
        )

        # NOTE (duplicate window): the target container is created and started
        # BEFORE the source is stopped (step 6 below). For idempotent stateless
        # services this minimizes downtime; for anything consuming a queue or
        # holding a lock/singleton resource, this means both instances can be
        # live simultaneously and may double-process work. There is no
        # automatic rollback: if creating/starting the target fails, the
        # source is untouched and left running (safe). If the target starts
        # fine but stopping the source subsequently fails, both instances are
        # left running -- see the explicit log below when that happens.
        created = dst.containers.run(
            image_ref,
            command=cfg["command"],
            name=target_name,
            detach=True,
            environment=cfg["environment"] or None,
            labels=cfg["labels"] or None,
            hostname=cfg["hostname"] or None,
            user=cfg["user"],
            working_dir=cfg["working_dir"],
            ports=cfg["ports"],
            volumes=cfg["volumes"],
            restart_policy=cfg["restart_policy"],
        )

        try:
            self._wait_until_ready(created)
        except Exception:
            log.exception(
                "Target container %r on %r did not become ready; leaving source %r on %r "
                "running untouched",
                target_name or created.id[:12],
                move.target_host,
                move.workload_id,
                move.source_host,
            )
            raise

        log.info("Target container started: id=%s status=%s", created.id[:12], created.status)

        # 6) Stop/remove source container
        log.info("Stopping source container %r on %r", move.workload_id, move.source_host)
        try:
            container.stop(timeout=self.settings.migration_stop_timeout_seconds)
            if self.settings.migration_remove_source:
                log.info("Removing source container %r on %r", move.workload_id, move.source_host)
                container.remove()
        except Exception:
            log.error(
                "DUPLICATE_INSTANCE: target %r is running on %r but stopping source %r on "
                "%r failed; both instances are now live. Manual cleanup required.",
                target_name or created.id[:12],
                move.target_host,
                move.workload_id,
                move.source_host,
            )
            raise

        log.info(
            "Migration complete: %s -> %s (%s)",
            move.source_host,
            move.target_host,
            move.workload_id,
        )

    def _wait_until_ready(self, container: Any) -> None:
        """Poll the target container until it reports `running`, or raise.

        A single post-create `reload()` (the old behaviour) can report success
        for a container that crash-loops moments later; polling for a short
        window catches that class of failure instead of reporting a false
        positive.
        """
        deadline = time.monotonic() + self.settings.migration_ready_timeout_seconds
        last_status = None

        while time.monotonic() < deadline:
            container.reload()
            last_status = container.status

            if last_status == "running":
                return

            if last_status in _TERMINAL_BAD_STATUSES:
                raise RuntimeError(
                    f"Target container entered terminal status {last_status!r} "
                    f"while waiting for readiness (id={container.id[:12]})"
                )

            time.sleep(self.settings.migration_ready_poll_seconds)

        raise RuntimeError(
            f"Target container did not reach 'running' within "
            f"{self.settings.migration_ready_timeout_seconds}s "
            f"(id={container.id[:12]}, last status={last_status!r})"
        )

    def _get_container(self, client: Any, name: str) -> Any:
        from docker.errors import NotFound

        try:
            return client.containers.get(name)
        except NotFound as err:
            # Try matching by prefix/id if needed later
            raise RuntimeError(f"Container {name!r} not found on source host") from err

    def _ensure_image_on_target(
        self, *, src: Any, dst: Any, image_ref: str, image_id: str | None
    ) -> None:
        # First: if image_ref looks like a tag, try pulling on target (fastest)
        if ":" in image_ref or "/" in image_ref:
            try:
                log.info("Ensuring image on target by pull: %s", image_ref)
                dst.images.pull(image_ref)
                return
            except Exception:
                log.warning("Pull failed for %s; falling back to save/load", image_ref)

        # Second: if target already has the image id, do nothing
        if image_id:
            try:
                dst.images.get(image_id)
                log.info("Target already has image id %s", image_id[:12])
                return
            except Exception:
                pass

        # Fallback: save from source and load into target (works for non-tagged images)
        log.info("Saving image from source and loading into target (may be slow)")
        src_img = src.images.get(image_id or image_ref)

        # Docker SDK: image.save() yields tar stream chunks
        tar_stream = io.BytesIO()
        for chunk in src_img.save(named=True):
            tar_stream.write(chunk)
        tar_stream.seek(0)

        dst.images.load(tar_stream.read())
        log.info("Image loaded onto target")
