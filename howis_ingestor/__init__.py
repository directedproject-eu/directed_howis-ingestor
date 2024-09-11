from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version('directed_howis-ingestor')
except PackageNotFoundError:
    __version__ = '(local)'

del PackageNotFoundError
del version
