import os
import sys
from unittest.mock import patch, Mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ingestion.s3.client import get_s3_client


def test_get_s3_client_calls_boto3_client():
    fake_client = Mock()

    with patch("ingestion.s3.client.boto3.client", return_value=fake_client) as p:
        c = get_s3_client()
        assert c is fake_client
        p.assert_called_once_with("s3")