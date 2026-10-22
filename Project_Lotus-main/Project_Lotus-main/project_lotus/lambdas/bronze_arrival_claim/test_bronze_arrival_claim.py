"""Unit tests for automatic arrival and manual folder-replay claims."""

import importlib.util
import io
import json
import os
import pathlib
import sys
import types
import unittest
from unittest.mock import MagicMock, patch


ENV = {
    "CLAIM_BUCKET": "lotus-sandbox-bronze-landing-data",
    "CLAIM_PREFIX": "control/bronze-arrival-claims",
    "ENABLED_DATASETS": "account_changes_batch,device_lookup_batch",
}


class FakeClientError(Exception):
    def __init__(self, response, operation_name):
        super().__init__(response["Error"]["Code"])
        self.response = response
        self.operation_name = operation_name


def load_module():
    path = pathlib.Path(__file__).with_name("bronze_arrival_claim.py")
    spec = importlib.util.spec_from_file_location(
        "bronze_arrival_claim_under_test", path
    )
    module = importlib.util.module_from_spec(spec)
    boto3 = types.ModuleType("boto3")
    boto3.client = MagicMock(return_value=MagicMock())
    botocore = types.ModuleType("botocore")
    exceptions = types.ModuleType("botocore.exceptions")
    exceptions.ClientError = FakeClientError
    with patch.dict(os.environ, ENV, clear=False), patch.dict(
        sys.modules,
        {
            "boto3": boto3,
            "botocore": botocore,
            "botocore.exceptions": exceptions,
        },
    ):
        spec.loader.exec_module(module)
    return module


class FakeS3:
    def __init__(self, client_error):
        self.client_error = client_error
        self.objects = {}

    def put_object(self, *, Bucket, Key, Body, ContentType, IfNoneMatch):
        identity = (Bucket, Key)
        if identity in self.objects:
            raise self.client_error(
                {"Error": {"Code": "PreconditionFailed"}}, "PutObject"
            )
        self.objects[identity] = bytes(Body)
        return {}

    def get_object(self, *, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}


class BronzeArrivalClaimTests(unittest.TestCase):
    def setUp(self):
        self.module = load_module()
        self.s3 = FakeS3(self.module.ClientError)
        self.module.s3 = self.s3

    def automatic_event(self, owner="execution-1"):
        return {
            "dataset_name": "account_changes_batch",
            "run_date": "2026-10-22",
            "folder_path": (
                "s3://lotus-sandbox-bronze-landing-data/bronze/"
                "account_changes_batch/2026-10-22/"
            ),
            "trigger_type": "automatic",
            "reprocess_id": "",
            "owner": owner,
        }

    def manual_event(self, reprocess_id="console-replay-001", owner="execution-1"):
        return {
            "folder_path": (
                "s3://lotus-sandbox-bronze-landing-data/bronze/"
                "account_changes_batch/2026-10-22"
            ),
            "trigger_type": "manual",
            "reprocess_id": reprocess_id,
            "owner": owner,
        }

    def test_automatic_claim_keeps_existing_dataset_date_key(self):
        result = self.module.lambda_handler(self.automatic_event(), None)
        self.assertTrue(result["claimed"])
        self.assertEqual(result["trigger_type"], "automatic")
        self.assertEqual(result["reprocess_id"], "")
        self.assertIn(
            (
                ENV["CLAIM_BUCKET"],
                "control/bronze-arrival-claims/account_changes_batch/"
                "2026-10-22.json",
            ),
            self.s3.objects,
        )

    def test_manual_claim_derives_and_normalizes_complete_folder(self):
        result = self.module.lambda_handler(self.manual_event(), None)
        self.assertTrue(result["claimed"])
        self.assertEqual(result["dataset_name"], "account_changes_batch")
        self.assertEqual(result["run_date"], "2026-10-22")
        self.assertEqual(
            result["folder_path"],
            "s3://lotus-sandbox-bronze-landing-data/bronze/"
            "account_changes_batch/2026-10-22/",
        )
        self.assertIn("/manual/account_changes_batch/2026-10-22/", result["claim_uri"])

    def test_manual_retry_is_idempotent_but_new_execution_does_not_steal_claim(self):
        first = self.module.lambda_handler(self.manual_event(), None)
        retry = self.module.lambda_handler(self.manual_event(), None)
        competing = self.module.lambda_handler(
            self.manual_event(owner="execution-2"), None
        )
        self.assertTrue(first["claimed"])
        self.assertTrue(retry["claimed"])
        self.assertFalse(competing["claimed"])

    def test_new_manual_execution_name_gets_a_distinct_replay_claim(self):
        first = self.module.lambda_handler(self.manual_event("console-replay-001"), None)
        second = self.module.lambda_handler(self.manual_event("console-replay-002"), None)
        self.assertTrue(first["claimed"])
        self.assertTrue(second["claimed"])
        self.assertNotEqual(first["claim_uri"], second["claim_uri"])

    def test_manual_input_rejects_file_and_wrong_bucket(self):
        file_event = self.manual_event()
        file_event["folder_path"] += "/part-000.parquet"
        with self.assertRaisesRegex(ValueError, "must be exactly"):
            self.module.lambda_handler(file_event, None)

        wrong_bucket = self.manual_event()
        wrong_bucket["folder_path"] = wrong_bucket["folder_path"].replace(
            ENV["CLAIM_BUCKET"], "unapproved-bucket"
        )
        with self.assertRaisesRegex(ValueError, "Bronze landing bucket"):
            self.module.lambda_handler(wrong_bucket, None)

    def test_manual_input_requires_reprocess_id(self):
        with self.assertRaisesRegex(ValueError, "requires reprocess_id"):
            self.module.lambda_handler(self.manual_event(reprocess_id=""), None)


if __name__ == "__main__":
    unittest.main()
