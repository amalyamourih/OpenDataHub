import os
import sys
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ingestion.s3.io import read_s3_object, write_s3_object, delete_s3_object


def test_read_s3_object_reads_bytes_from_body():
    fake_body = Mock()
    fake_body.read.return_value = b"hello"

    fake_s3 = Mock()
    fake_s3.get_object.return_value = {"Body": fake_body}

    with patch("ingestion.s3.io.get_s3_client", return_value=fake_s3), \
         patch("ingestion.s3.io.S3_BUCKET", "test-bucket"):
        data = read_s3_object("k1")

    assert data == b"hello"
    fake_s3.get_object.assert_called_once_with(Bucket="test-bucket", Key="k1")
    fake_body.read.assert_called_once()


def test_write_s3_object_calls_put_object():
    fake_s3 = Mock()

    with patch("ingestion.s3.io.get_s3_client", return_value=fake_s3), \
         patch("ingestion.s3.io.S3_BUCKET", "test-bucket"):
        write_s3_object("k2", b"data")

    fake_s3.put_object.assert_called_once_with(Bucket="test-bucket", Key="k2", Body=b"data")


def test_delete_s3_object_calls_delete_object():
    fake_s3 = Mock()

    with patch("ingestion.s3.io.get_s3_client", return_value=fake_s3), \
         patch("ingestion.s3.io.S3_BUCKET", "test-bucket"):
        delete_s3_object("k3")

    fake_s3.delete_object.assert_called_once_with(Bucket="test-bucket", Key="k3")