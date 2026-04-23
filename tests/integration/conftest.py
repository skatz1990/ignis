import pathlib

import pytest

_FIXTURES = pathlib.Path(__file__).parent.parent / "fixtures"
_SKEW = _FIXTURES / "skew_example.ndjson"

_BUCKET = "ignis-test"
_AZURITE_ACCOUNT = "devstoreaccount1"
# Well-known Azurite default key (publicly documented).
_AZURITE_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)


@pytest.fixture(scope="session")
def s3_fixture():
    pytest.importorskip("s3fs")
    MinioContainer = pytest.importorskip("testcontainers.minio").MinioContainer
    import s3fs

    try:
        minio = MinioContainer()
        minio.start()
    except Exception as exc:
        pytest.skip(f"MinIO container failed to start: {exc}")

    try:
        config = minio.get_config()
        endpoint_url = f"http://{config['endpoint']}"
        opts = {
            "key": config["access_key"],
            "secret": config["secret_key"],
            "client_kwargs": {"endpoint_url": endpoint_url},
        }
        fs = s3fs.S3FileSystem(**opts)
        fs.mkdir(_BUCKET)
        fs.pipe(f"{_BUCKET}/skew_example.ndjson", _SKEW.read_bytes())
        yield f"s3://{_BUCKET}/skew_example.ndjson", opts
    finally:
        minio.stop()


@pytest.fixture(scope="session")
def gcs_fixture():
    pytest.importorskip("gcsfs")
    DockerContainer = pytest.importorskip("testcontainers.core.container").DockerContainer
    LogMessageWaitStrategy = pytest.importorskip(
        "testcontainers.core.wait_strategies"
    ).LogMessageWaitStrategy
    import gcsfs

    try:
        container = (
            DockerContainer(
                "fsouza/fake-gcs-server",
                _wait_strategy=LogMessageWaitStrategy("server started"),
            )
            .with_command("-scheme http -port 4443")
            .with_exposed_ports(4443)
        )
        container.start()
    except Exception as exc:
        pytest.skip(f"fake-gcs-server container failed to start: {exc}")

    try:
        port = container.get_exposed_port(4443)
        endpoint_url = f"http://localhost:{port}"
        opts = {"project": "test-project", "endpoint_url": endpoint_url, "token": "anon"}
        fs = gcsfs.GCSFileSystem(**opts)
        fs.mkdir(_BUCKET)
        fs.pipe(f"{_BUCKET}/skew_example.ndjson", _SKEW.read_bytes())
        yield f"gs://{_BUCKET}/skew_example.ndjson", opts
    finally:
        container.stop()


@pytest.fixture(scope="session")
def azure_fixture():
    pytest.importorskip("adlfs")
    DockerContainer = pytest.importorskip("testcontainers.core.container").DockerContainer
    LogMessageWaitStrategy = pytest.importorskip(
        "testcontainers.core.wait_strategies"
    ).LogMessageWaitStrategy
    import adlfs

    try:
        container = (
            DockerContainer(
                "mcr.microsoft.com/azure-storage/azurite",
                _wait_strategy=LogMessageWaitStrategy("successfully listens"),
            )
            .with_command("azurite-blob --blobHost 0.0.0.0 --loose --skipApiVersionCheck")
            .with_exposed_ports(10000)
        )
        container.start()
    except Exception as exc:
        pytest.skip(f"Azurite container failed to start: {exc}")

    try:
        port = container.get_exposed_port(10000)
        conn_str = (
            f"DefaultEndpointsProtocol=http;"
            f"AccountName={_AZURITE_ACCOUNT};"
            f"AccountKey={_AZURITE_KEY};"
            f"BlobEndpoint=http://localhost:{port}/{_AZURITE_ACCOUNT};"
        )
        opts = {"connection_string": conn_str}
        fs = adlfs.AzureBlobFileSystem(**opts)
        fs.mkdir(_BUCKET)
        fs.pipe(f"{_BUCKET}/skew_example.ndjson", _SKEW.read_bytes())
        yield f"abfs://{_BUCKET}/skew_example.ndjson", opts
    finally:
        container.stop()
