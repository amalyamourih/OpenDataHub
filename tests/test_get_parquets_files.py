import os
import sys
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ingestion.s3.get_parquets_files import list_s3_keys


def test_list_s3_keys_filters_folder_keys():
    fake_paginator = Mock()
    fake_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "parquets_files/tabular/a.parquet"},
                {"Key": "parquets_files/tabular/"},
                {"Key": "parquets_files/others/b.parquet"},
            ]
        }
    ]

    fake_s3 = Mock()
    fake_s3.get_paginator.return_value = fake_paginator

    with patch("ingestion.s3.get_parquets_files.s3_client", fake_s3):
        keys = list_s3_keys("my-bucket", prefix="parquets_files/")

    assert keys == [
        "parquets_files/tabular/a.parquet",
        "parquets_files/others/b.parquet",
    ]
    fake_s3.get_paginator.assert_called_once_with("list_objects_v2")
    fake_paginator.paginate.assert_called_once_with(Bucket="my-bucket", Prefix="parquets_files/")


def test_list_s3_keys_handles_empty_contents():
    fake_paginator = Mock()
    fake_paginator.paginate.return_value = [{"Contents": []}, {}]

    fake_s3 = Mock()
    fake_s3.get_paginator.return_value = fake_paginator

    with patch("ingestion.s3.get_parquets_files.s3_client", fake_s3):
        keys = list_s3_keys("my-bucket", prefix="parquets_files/")

    assert keys == []