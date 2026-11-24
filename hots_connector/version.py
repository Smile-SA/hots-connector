from importlib.metadata import PackageNotFoundError, version


def _read_version() -> str:
    # VERSION.txt is the single source; it reaches the installed package
    # metadata through pyproject's dynamic version.
    try:
        return version("hots-connector")
    except PackageNotFoundError:  # imported from a checkout that isn't installed
        return "0.0.0"


__version__ = _read_version()
