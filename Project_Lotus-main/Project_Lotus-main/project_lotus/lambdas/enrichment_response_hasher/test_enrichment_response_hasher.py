import gzip
import hashlib
import io
import json
import unittest

from enrichment_response_hasher import (
    OUTPUT_PREFIX,
    canonical_msisdn,
    hash_envelope,
    process_message,
)


class FakeS3:
    def __init__(self, source, key="enterprise-enrichment/responses/provider=idv/sample.gz"):
        self.source = source
        self.key = key
        self.written = {}

    def get_object(self, *, Bucket, Key, VersionId):
        assert (Bucket, Key, VersionId) == (
            "landing", self.key, "v1"
        )
        return {"Body": io.BytesIO(self.source)}

    def list_objects_v2(self, *, Bucket, Prefix, MaxKeys):
        assert MaxKeys == 1
        matches = [key for bucket, key in self.written if bucket == Bucket and key.startswith(Prefix)]
        return {"Contents": [{"Key": key} for key in matches[:1]]} if matches else {}

    def put_object(self, *, Bucket, Key, Body, **_kwargs):
        self.written[(Bucket, Key)] = Body


class FakeSQS:
    def __init__(self):
        self.messages = []

    def send_message(self, *, QueueUrl, MessageBody):
        assert QueueUrl == "queue"
        self.messages.append(MessageBody)


class FakeSecrets:
    def get_secret_value(self, *, SecretId):
        return {"SecretString": {"ac": "ac-salt", "at": "at-salt"}[SecretId]}


class Context:
    def get_remaining_time_in_millis(self):
        return 60_000


class ResponseHasherTests(unittest.TestCase):
    @staticmethod
    def event_for(key):
        return json.dumps({
            "source": "aws.s3",
            "detail-type": "Object Created",
            "detail": {
                "bucket": {"name": "landing"},
                "object": {"key": key, "version-id": "v1"},
            },
        })

    def test_canonicalization_and_exact_client_hash_formula(self):
        self.assertEqual(canonical_msisdn("(416) 555-0100"), "+14165550100")
        self.assertEqual(canonical_msisdn("14165550100"), "+14165550100")
        envelope = {
            "source_name": "idv",
            "payload": '{"responseCode":"1"}',
            "items": [{"msisdn": "+14165550100"}],
        }
        count = hash_envelope(envelope, "idv", "ac-salt", "at-salt")
        self.assertEqual(count, 1)
        self.assertEqual(envelope["payload"], '{"responseCode":"1"}')
        self.assertEqual(
            envelope["items"][0]["phone_number_AC_hash"],
            hashlib.sha256(b"+14165550100.ac-salt").hexdigest(),
        )
        self.assertEqual(
            envelope["items"][0]["phone_number_AT_hash"],
            hashlib.sha256(b"+14165550100.at-salt").hexdigest(),
        )

    def test_chunk_continuation_and_completion(self):
        rows = [
            {"source_name": "idv", "payload": "{}", "items": [{"msisdn": f"+1416555010{i}"}]}
            for i in range(3)
        ]
        source = gzip.compress(b"".join(json.dumps(row).encode() + b"\n" for row in rows))
        s3 = FakeS3(source)
        queue = FakeSQS()
        event = {
            "source": "aws.s3",
            "detail-type": "Object Created",
            "detail": {
                "bucket": {"name": "landing"},
                "object": {
                    "key": "enterprise-enrichment/responses/provider=idv/sample.gz",
                    "version-id": "v1",
                },
            },
        }
        body = json.dumps(event)
        statuses = []
        for _ in range(3):
            result = process_message(
                body, Context(), s3_client=s3, sqs_client=queue,
                secret_client=FakeSecrets(), input_bucket="landing",
                output_bucket="landing", queue_url="queue",
                ac_secret_id="ac", at_secret_id="at", max_envelopes=1,
            )
            statuses.append(result["status"])
            if queue.messages:
                body = queue.messages.pop(0)
        self.assertEqual(statuses, ["continued", "continued", "complete"])
        prefix = f"{OUTPUT_PREFIX}provider=idv/source_id={result['source_id']}/"
        self.assertEqual(
            json.loads(s3.written[("landing", prefix + "_SUCCESS.json")])["part_count"], 3
        )
        for index in range(3):
            part = s3.written[("landing", prefix + f"part-{index:06d}.jsonl.gz")]
            hashed = json.loads(gzip.decompress(part))
            self.assertIn("phone_number_AC_hash", hashed["items"][0])
        repeat = process_message(
            json.dumps(event), Context(), s3_client=s3, sqs_client=queue,
            secret_client=FakeSecrets(), input_bucket="landing",
            output_bucket="landing", queue_url="queue",
            ac_secret_id="ac", at_secret_id="at", max_envelopes=1,
        )
        self.assertEqual(repeat["status"], "already_complete")

    def test_missing_item_msisdn_fails_before_writing_part(self):
        row = {"source_name": "portps", "items": [{"item_index": 1}], "payload": "<root/>"}
        with self.assertRaises(ValueError):
            hash_envelope(row, "portps", "ac", "at")

    def test_plain_ndjson_array_pretty_json_and_gzip_by_magic(self):
        rows = [
            {"source_name": "idv", "items": [{"msisdn": f"+1416555010{i}"}]}
            for i in range(3)
        ]
        cases = [
            ("responses.jsonl", b"".join(json.dumps(row).encode() + b"\n" for row in rows)),
            ("responses.json", json.dumps(rows, indent=2).encode()),
            ("response", json.dumps(rows[0], indent=2).encode()),
            ("compressed.json", gzip.compress(json.dumps(rows).encode())),
        ]
        for filename, data in cases:
            with self.subTest(filename=filename):
                key = "enterprise-enrichment/responses/provider=idv/" + filename
                s3 = FakeS3(data, key)
                queue = FakeSQS()
                body = self.event_for(key)
                results = []
                while True:
                    result = process_message(
                        body, Context(), s3_client=s3, sqs_client=queue,
                        secret_client=FakeSecrets(), input_bucket="landing",
                        output_bucket="landing", queue_url="queue",
                        ac_secret_id="ac", at_secret_id="at", max_envelopes=2,
                    )
                    results.append(result)
                    if result["status"] == "complete":
                        break
                    body = queue.messages.pop(0)
                self.assertEqual(sum(row["items"] for row in results), 1 if filename == "response" else 3)
                self.assertTrue(any(key.endswith("_SUCCESS.json") for _, key in s3.written))

    def test_invalid_json_is_not_marked_complete(self):
        key = "enterprise-enrichment/responses/provider=idv/invalid.json"
        for data in (b"not json", b"", b'[{"source_name":"idv"}'):
            with self.subTest(data=data):
                s3 = FakeS3(data, key)
                with self.assertRaises(ValueError):
                    process_message(
                        self.event_for(key), Context(), s3_client=s3, sqs_client=FakeSQS(),
                        secret_client=FakeSecrets(), input_bucket="landing",
                        output_bucket="landing", queue_url="queue",
                        ac_secret_id="ac", at_secret_id="at",
                    )
                self.assertFalse(any(key.endswith("_SUCCESS.json") for _, key in s3.written))

    def test_provider_control_object_is_ignored(self):
        key = "enterprise-enrichment/responses/provider=idv/_SUCCESS.json"
        s3 = FakeS3(b"{}", key)
        result = process_message(
            self.event_for(key), Context(), s3_client=s3, sqs_client=FakeSQS(),
            secret_client=FakeSecrets(), input_bucket="landing",
            output_bucket="landing", queue_url="queue",
            ac_secret_id="ac", at_secret_id="at",
        )
        self.assertEqual(result["status"], "ignored_control_object")
        self.assertEqual(s3.written, {})

    def test_wrong_response_prefix_is_rejected_with_guidance(self):
        key = "enterprise-enrichment/requests/provider=idv/request.json"
        s3 = FakeS3(b"{}", key)
        with self.assertRaisesRegex(ValueError, "detail.object.key must start with"):
            process_message(
                self.event_for(key), Context(), s3_client=s3, sqs_client=FakeSQS(),
                secret_client=FakeSecrets(), input_bucket="landing",
                output_bucket="landing", queue_url="queue",
                ac_secret_id="ac", at_secret_id="at",
            )


if __name__ == "__main__":
    unittest.main()
