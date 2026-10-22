"""Hash MSISDNs in the client's JSON PortPS and IDV responses.

The source object remains immutable. Each SQS invocation writes one deterministic
gzip part and sends a cursor for the next part. A completion marker is written
only after the last part; Silver must consume only completed prefixes.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import logging
import os
import re


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

INPUT_PREFIX = "enterprise-enrichment/responses/"
OUTPUT_PREFIX = "enterprise-enrichment/hashed-responses/"
PROVIDERS = {"portps", "idv"}
MSISDN_SEPARATORS = re.compile(r"[\s.()\-]")
MAX_JSON_VALUE_CHARS = 10 * 1024 * 1024
MAX_PART_BYTES = 32 * 1024 * 1024
MIN_REMAINING_MS = 20_000


def canonical_msisdn(value: object) -> str:
    """Use the Canadian E.164 input used by the client submission and IDV jobs."""
    if not isinstance(value, str):
        raise ValueError("response item has no string MSISDN")
    number = MSISDN_SEPARATORS.sub("", value)
    if re.fullmatch(r"\+1\d{10}", number):
        return number
    if re.fullmatch(r"1\d{10}", number):
        return "+" + number
    if re.fullmatch(r"\d{10}", number):
        return "+1" + number
    raise ValueError("response item has invalid Canadian MSISDN")


def salted_hash(number: str, salt: str) -> str:
    return hashlib.sha256(f"{number}.{salt}".encode("utf-8")).hexdigest()


def hash_envelope(envelope: dict, provider: str, ac_salt: str, at_salt: str) -> int:
    """Add hashes to each item; leave the signed/raw provider payload untouched."""
    if envelope.get("source_name") != provider:
        raise ValueError("response source_name does not match landing provider")
    items = envelope.get("items")
    if not isinstance(items, list):
        raise ValueError("response envelope has no items array")
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("response item must be an object")
        number = canonical_msisdn(item.get("msisdn"))
        item["phone_number_AC_hash"] = salted_hash(number, ac_salt)
        item["phone_number_AT_hash"] = salted_hash(number, at_salt)
    return len(items)


def _provider_from_key(key: str) -> str:
    for provider in PROVIDERS:
        prefix = f"{INPUT_PREFIX}provider={provider}/"
        filename = key.rsplit("/", 1)[-1]
        if key.startswith(prefix) and filename and not filename.startswith("_"):
            return provider
    raise ValueError(
        "detail.object.key must start with "
        "enterprise-enrichment/responses/provider=portps/ or "
        "enterprise-enrichment/responses/provider=idv/; "
        "provide the S3 object key, not an s3:// URI or customer input-file key"
    )


class _PrefixedReader(io.RawIOBase):
    """Restore bytes used to detect gzip without seeking the S3 response body."""

    def __init__(self, prefix: bytes, stream):
        self._prefix = memoryview(prefix)
        self._stream = stream

    def readable(self):
        return True

    def readinto(self, target):
        size = min(len(self._prefix), len(target))
        if size:
            target[:size] = self._prefix[:size]
            self._prefix = self._prefix[size:]
        if size < len(target):
            chunk = self._stream.read(len(target) - size)
            target[size:size + len(chunk)] = chunk
            size += len(chunk)
        return size


def _iter_json_values(source):
    """Stream a JSON array, one JSON object, or successive NDJSON objects."""
    decoder = json.JSONDecoder()
    buffer = ""
    eof = False
    array_mode = None
    array_needs_separator = False
    array_allows_end = True
    seen_value = False

    def more():
        nonlocal buffer, eof
        chunk = source.read(64 * 1024)
        if chunk:
            buffer += chunk
            if len(buffer) > MAX_JSON_VALUE_CHARS:
                raise ValueError("response JSON value exceeds maximum size")
        else:
            eof = True

    while True:
        buffer = buffer.lstrip()
        if not buffer:
            if eof:
                if array_mode:
                    raise ValueError("response JSON array is incomplete")
                if not seen_value:
                    raise ValueError("response JSON file is empty")
                return
            more()
            continue

        if array_mode is None:
            array_mode = buffer[0] == "["
            if array_mode:
                buffer = buffer[1:]
                continue

        if array_mode and array_needs_separator:
            if buffer[0] == ",":
                buffer = buffer[1:]
                array_needs_separator = False
                array_allows_end = False
                continue
            if buffer[0] == "]":
                buffer = buffer[1:]
                while True:
                    if buffer.strip():
                        raise ValueError("unexpected content after response JSON array")
                    if eof:
                        return
                    buffer = ""
                    more()
            raise ValueError("response JSON array needs a comma or closing bracket")

        if array_mode and buffer[0] == "]":
            if not array_allows_end:
                raise ValueError("response JSON array has a trailing comma")
            buffer = buffer[1:]
            while True:
                if buffer.strip():
                    raise ValueError("unexpected content after response JSON array")
                if eof:
                    return
                buffer = ""
                more()

        try:
            value, end = decoder.raw_decode(buffer)
        except json.JSONDecodeError as exc:
            if eof:
                raise ValueError("response is not valid JSON") from exc
            more()
            continue
        buffer = buffer[end:]
        if array_mode:
            array_needs_separator = True
        seen_value = True
        yield value


def _source_id(bucket: str, key: str, version_id: str) -> str:
    return hashlib.sha256(f"{bucket}\n{key}\n{version_id}".encode()).hexdigest()


def _is_provider_control_object(message: dict, input_bucket: str) -> bool:
    if message.get("source") != "aws.s3" or message.get("detail-type") != "Object Created":
        return False
    if message.get("detail", {}).get("bucket", {}).get("name") != input_bucket:
        return False
    key = message.get("detail", {}).get("object", {}).get("key")
    if not isinstance(key, str):
        return False
    filename = key.rsplit("/", 1)[-1]
    return any(
        key.startswith(f"{INPUT_PREFIX}provider={provider}/")
        and (not filename or filename.startswith("_"))
        for provider in PROVIDERS
    )


def _decode_message(body: str, input_bucket: str) -> dict:
    message = json.loads(body)
    if message.get("type") == "continue":
        bucket = message["bucket"]
        key = message["key"]
        version_id = message["version_id"]
        line_offset = message["line_offset"]
        part_index = message["part_index"]
    else:
        if message.get("source") != "aws.s3" or message.get("detail-type") != "Object Created":
            raise ValueError("expected an S3 Object Created EventBridge event")
        detail = message["detail"]
        bucket = detail["bucket"]["name"]
        key = detail["object"]["key"]
        version_id = detail["object"].get("version-id")
        line_offset = 0
        part_index = 0
    if bucket != input_bucket:
        raise ValueError("response bucket is not the configured landing bucket")
    provider = _provider_from_key(key)
    if not isinstance(version_id, str) or not version_id:
        raise ValueError("response event must carry an S3 version ID")
    if type(line_offset) is not int or type(part_index) is not int:
        raise ValueError("invalid continuation cursor")
    if line_offset < 0 or part_index < 0:
        raise ValueError("negative continuation cursor")
    source_id = _source_id(bucket, key, version_id)
    if message.get("type") == "continue" and message.get("source_id") != source_id:
        raise ValueError("continuation source ID does not match object version")
    return {
        "bucket": bucket,
        "key": key,
        "version_id": version_id,
        "provider": provider,
        "line_offset": line_offset,
        "part_index": part_index,
        "source_id": source_id,
    }


def _secret(secret_client, secret_id: str) -> str:
    value = secret_client.get_secret_value(SecretId=secret_id).get("SecretString")
    if not isinstance(value, str) or not value:
        raise ValueError("hashing secret must contain a nonempty SecretString")
    return value


def _exists(s3_client, bucket: str, key: str) -> bool:
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
    return any(obj["Key"] == key for obj in response.get("Contents", []))


def process_message(
    body: str,
    context,
    *,
    s3_client,
    sqs_client,
    secret_client,
    input_bucket: str,
    output_bucket: str,
    queue_url: str,
    ac_secret_id: str,
    at_secret_id: str,
    max_envelopes: int = 100,
) -> dict:
    if max_envelopes < 1:
        raise ValueError("MAX_ENVELOPES must be positive")
    if _is_provider_control_object(json.loads(body), input_bucket):
        LOGGER.info("ignored provider control object")
        return {"status": "ignored_control_object"}
    cursor = _decode_message(body, input_bucket)
    prefix = f"{OUTPUT_PREFIX}provider={cursor['provider']}/source_id={cursor['source_id']}/"
    done_key = prefix + "_SUCCESS.json"
    if _exists(s3_client, output_bucket, done_key):
        return {"status": "already_complete", "source_id": cursor["source_id"]}

    ac_salt = _secret(secret_client, ac_secret_id)
    at_salt = _secret(secret_client, at_secret_id)
    response = s3_client.get_object(
        Bucket=cursor["bucket"], Key=cursor["key"], VersionId=cursor["version_id"]
    )
    lines: list[bytes] = []
    part_bytes = 0
    item_count = 0
    at_eof = False
    stream = response["Body"]
    try:
        prefix_bytes = stream.read(2)
        with io.BufferedReader(_PrefixedReader(prefix_bytes, stream)) as buffered:
            binary_source = (
                gzip.GzipFile(fileobj=buffered, mode="rb")
                if prefix_bytes == b"\x1f\x8b" else buffered
            )
            with io.TextIOWrapper(binary_source, encoding="utf-8-sig") as source:
                envelopes = _iter_json_values(source)
                sentinel = object()
                for _ in range(cursor["line_offset"]):
                    if next(envelopes, sentinel) is sentinel:
                        raise ValueError("continuation cursor exceeds response length")
                while len(lines) < max_envelopes:
                    if lines and context.get_remaining_time_in_millis() < MIN_REMAINING_MS:
                        break
                    if lines and part_bytes >= MAX_PART_BYTES:
                        break
                    envelope = next(envelopes, sentinel)
                    if envelope is sentinel:
                        at_eof = True
                        break
                    if not isinstance(envelope, dict):
                        raise ValueError("response envelope must be an object")
                    item_count += hash_envelope(
                        envelope, cursor["provider"], ac_salt, at_salt
                    )
                    encoded = json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode("utf-8") + b"\n"
                    lines.append(encoded)
                    part_bytes += len(encoded)
                if not at_eof and len(lines) == max_envelopes:
                    at_eof = next(envelopes, sentinel) is sentinel
    finally:
        stream.close()

    if lines:
        part_key = prefix + f"part-{cursor['part_index']:06d}.jsonl.gz"
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as dest:
            dest.writelines(lines)
        s3_client.put_object(
            Bucket=output_bucket,
            Key=part_key,
            Body=buffer.getvalue(),
            ContentType="application/x-ndjson",
            ContentEncoding="gzip",
            ServerSideEncryption="AES256",
        )

    next_offset = cursor["line_offset"] + len(lines)
    if at_eof:
        marker = {
            "provider": cursor["provider"],
            "source_id": cursor["source_id"],
            "source_bucket": cursor["bucket"],
            "source_key": cursor["key"],
            "source_version_id": cursor["version_id"],
            "envelope_count": next_offset,
            "part_count": cursor["part_index"] + int(bool(lines)),
            "hash_input_format": "CA_E164",
        }
        s3_client.put_object(
            Bucket=output_bucket,
            Key=done_key,
            Body=json.dumps(marker, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json",
            ServerSideEncryption="AES256",
        )
        status = "complete"
    else:
        continuation = {
            "type": "continue",
            "bucket": cursor["bucket"],
            "key": cursor["key"],
            "version_id": cursor["version_id"],
            "provider": cursor["provider"],
            "line_offset": next_offset,
            "part_index": cursor["part_index"] + 1,
            "source_id": cursor["source_id"],
        }
        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(continuation, separators=(",", ":")),
        )
        status = "continued"
    LOGGER.info(
        "response hashing %s provider=%s source_id=%s envelopes=%d items=%d",
        status, cursor["provider"], cursor["source_id"], len(lines), item_count,
    )
    return {"status": status, "source_id": cursor["source_id"], "envelopes": len(lines), "items": item_count}


def lambda_handler(event, context):
    import boto3

    s3_client = boto3.client("s3")
    sqs_client = boto3.client("sqs")
    secret_client = boto3.client("secretsmanager")
    required = (
        "INPUT_BUCKET", "OUTPUT_BUCKET", "CONTINUATION_QUEUE_URL",
        "AC_SECRET_ID", "AT_SECRET_ID",
    )
    settings = {name: os.environ[name] for name in required}
    results = []
    for record in event["Records"]:
        if record.get("eventSource") != "aws:sqs":
            raise ValueError("expected an SQS event")
        results.append(
            process_message(
                record["body"], context,
                s3_client=s3_client,
                sqs_client=sqs_client,
                secret_client=secret_client,
                input_bucket=settings["INPUT_BUCKET"],
                output_bucket=settings["OUTPUT_BUCKET"],
                queue_url=settings["CONTINUATION_QUEUE_URL"],
                ac_secret_id=settings["AC_SECRET_ID"],
                at_secret_id=settings["AT_SECRET_ID"],
                max_envelopes=int(os.environ.get("MAX_ENVELOPES", "100")),
            )
        )
    return {"results": results}
