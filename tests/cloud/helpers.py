import pathlib

import fsspec
import pytest
from fsspec.implementations.memory import MemoryFileSystem

SKEW_FIXTURE = pathlib.Path(__file__).parent.parent / "fixtures" / "skew_example.ndjson"
PARTITION_FIXTURE = pathlib.Path(__file__).parent.parent / "fixtures" / "partition_example.ndjson"

_FIXTURES = [
    ("skew_example.ndjson", SKEW_FIXTURE),
    ("partition_example.ndjson", PARTITION_FIXTURE),
]


def make_cloud_fs_fixture(protocols: tuple[str, ...], real_impl: str):
    """
    Return a pytest fixture that registers an in-memory filesystem for the
    given protocols, populates it with test fixtures, and restores the real
    implementation on teardown.
    """

    class _MemoryCloud(MemoryFileSystem):
        protocol = protocols

        @classmethod
        def _strip_protocol(cls, path):
            if isinstance(path, list):
                return [cls._strip_protocol(p) for p in path]
            for proto in sorted(protocols, key=len, reverse=True):
                if path.startswith(f"{proto}://"):
                    return "/" + path[len(proto) + 3 :]
            return path

    @pytest.fixture(autouse=False)
    def _fixture():
        fs = _MemoryCloud()
        fs.store.clear()
        for name, fixture_path in _FIXTURES:
            fs.pipe(f"/test-container/logs/{name}", fixture_path.read_bytes())
        for proto in protocols:
            fsspec.register_implementation(proto, _MemoryCloud, clobber=True)
        yield fs
        for proto in protocols:
            fsspec.register_implementation(proto, real_impl, clobber=True)

    return _fixture
