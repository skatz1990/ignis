from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("spark-ignis")
except PackageNotFoundError:
    __version__ = "unknown"
