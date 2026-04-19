"""acatome-quest-mcp — Paper-request MCP for scientific papers."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("acatome-quest-mcp")
except PackageNotFoundError:
    __version__ = "0.0.0-dev"
