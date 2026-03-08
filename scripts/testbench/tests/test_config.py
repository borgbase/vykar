import unittest
from unittest import mock

from vykar_testbench import config


class ConfigTests(unittest.TestCase):
    def test_ensure_backend_ready_skips_non_s3(self) -> None:
        with mock.patch.dict("sys.modules", {}):
            config.ensure_backend_ready("local", "/mnt/repos/scenario-repo")

    def test_ensure_backend_ready_creates_missing_s3_bucket(self) -> None:
        client = mock.Mock()
        client.bucket_exists.return_value = False

        minio_module = mock.Mock()
        minio_module.Minio.return_value = client

        with mock.patch.dict("sys.modules", {"minio": minio_module}):
            config.ensure_backend_ready("s3", "s3+http://127.0.0.1:9000/vykar-scenario/scenario-simple")

        minio_module.Minio.assert_called_once_with(
            "127.0.0.1:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
            region="us-east-1",
        )
        client.bucket_exists.assert_called_once_with("vykar-scenario")
        client.make_bucket.assert_called_once_with("vykar-scenario")

    def test_ensure_backend_ready_skips_create_when_bucket_exists(self) -> None:
        client = mock.Mock()
        client.bucket_exists.return_value = True

        minio_module = mock.Mock()
        minio_module.Minio.return_value = client

        with mock.patch.dict("sys.modules", {"minio": minio_module}):
            config.ensure_backend_ready("s3", "s3+http://127.0.0.1:9000/vykar-scenario/scenario-simple")

        client.make_bucket.assert_not_called()

    def test_ensure_backend_ready_raises_when_minio_missing(self) -> None:
        with mock.patch.dict("sys.modules", {"minio": None}):
            with self.assertRaisesRegex(RuntimeError, "minio package is not installed"):
                config.ensure_backend_ready("s3", "s3+http://127.0.0.1:9000/vykar-scenario/scenario-simple")
