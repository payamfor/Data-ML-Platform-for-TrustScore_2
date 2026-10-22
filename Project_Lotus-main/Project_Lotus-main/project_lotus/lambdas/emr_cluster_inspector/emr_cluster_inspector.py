"""Read EMR cluster state and count any steps that are not yet terminal."""

import boto3

emr = boto3.client("emr")

ACTIVE_STEP_STATES = ["PENDING", "RUNNING", "CANCEL_PENDING"]


def lambda_handler(event, context):
    cluster_id = event.get("cluster_id")
    if not isinstance(cluster_id, str) or not cluster_id.strip():
        raise ValueError("cluster_id is required")

    cluster = emr.describe_cluster(ClusterId=cluster_id)["Cluster"]
    active_step_count = 0
    marker = None

    while True:
        request = {
            "ClusterId": cluster_id,
            "StepStates": ACTIVE_STEP_STATES,
        }
        if marker:
            request["Marker"] = marker

        response = emr.list_steps(**request)
        active_step_count += len(response.get("Steps", []))
        marker = response.get("Marker")
        if not marker:
            break

    return {
        "cluster_id": cluster_id,
        "cluster_state": cluster["Status"]["State"],
        "active_step_count": active_step_count,
    }
