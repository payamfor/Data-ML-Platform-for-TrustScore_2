"""Pure, fail-closed recovery decisions. No Spark/AWS imports or data writes."""
from __future__ import annotations

import hashlib
import json

STAGES = ("prepare", "accounts", "customers", "validate", "publish")
TABLES = (
    "account_changes_canonical_events", "msisdn_lifecycle",
    "msisdn_lifecycle_account_mapping", "msisdn_lifecycle_account_customer_mapping",
    "normalized_canonical_events",
)


class RecoveryBlocked(RuntimeError):
    pass


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def build_plan(state, completed, journals, heads, statuses, rollback=None):
    """Build a fail-closed exact-snapshot rollback plan.

    Complete journals record *head dictionaries*, not bare snapshot IDs, in
    the supplied Gold ZIP. Comparing snapshot timestamps is deliberately absent.
    Exclusive external maintenance is still required: old publishers do not
    participate in the sidecar lease or tag snapshots with this run ID.
    """
    common, rollback_errors = [], []
    pins = state.get("pins", {}).get("sinks", {})
    if set(pins) != set(TABLES):
        common.append("The run must pin exactly the five supported Gold tables.")
    if set(heads) != set(TABLES):
        common.append("Could not inspect all five current Gold heads.")
    if "succeeded" in statuses and "failed" in statuses:
        common.append("Conflicting terminal control records require investigation.")
    if "succeeded" in statuses:
        rollback_errors.append("Run is already succeeded; rollback is not failure recovery.")
    if "failed" in statuses:
        if not rollback:
            rollback_errors.append("Run was already closed without this tool's verified rollback journal; investigate instead of mutating it.")
    if not statuses or not set(statuses) <= {"running", "failed", "succeeded"}:
        common.append("No valid control reservation exists for this staged run.")
    seen_missing = False
    for stage in STAGES:
        if not completed.get(stage):
            seen_missing = True
        elif seen_missing:
            common.append("Completed stages are not a contiguous prefix.")
    if completed.get("validate") and not completed["validate"].get("no_data"):
        if completed["validate"].get("passed") is not True:
            rollback_errors.append("Saved validation failed DQ; publish rollback evidence is invalid.")
    if rollback:
        if rollback.get("state_sha256") != digest(state):
            common.append("Rollback journal belongs to different staging state.")
        if set(rollback.get("tables", {})) != set(TABLES):
            common.append("Rollback journal does not contain all five tables.")
    report, uncertain = [], []
    for name in TABLES:
        pin, head, journal = pins.get(name), heads.get(name), journals.get(name)
        entry = {"name": name, "pin": pin, "current": head, "publish_status": (journal or {}).get("status")}
        if not isinstance(pin, dict) or not isinstance(head, dict):
            common.append(f"{name}: missing snapshot evidence.")
            report.append(entry)
            continue
        if pin.get("missing") or head.get("missing") or pin.get("table") != head.get("table"):
            common.append(f"{name}: missing/replaced target table.")
        expected = journal.get("head_after") if journal and journal.get("status") == "complete" else pin
        if journal and journal.get("status") not in ("writing", "complete"):
            common.append(f"{name}: unrecognized publish journal.")
        if journal and journal.get("status") == "complete" and not isinstance(expected, dict):
            common.append(f"{name}: invalid completed publish head.")
        if not completed.get("validate") and journal:
            common.append(f"{name}: publish intent exists without validation completion.")
        saved_item = rollback.get("tables", {}).get(name, {}) if rollback else {}
        if rollback and head == saved_item.get("target"):
            entry["classification"] = "restored"
        elif head == expected:
            entry["classification"] = "unchanged" if head == pin else "known_publish"
        elif journal and journal.get("status") == "writing":
            entry["classification"] = "uncertain_write"
            uncertain.append(name)
            rollback_errors.append(
                f"{name}: a writing journal and changed head cannot be attributed to this run."
            )
        else:
            entry["classification"] = "unexpected_head"
            rollback_errors.append(
                f"{name}: current head differs from its pin/completed journal."
            )
        if head != pin and pin.get("snapshot_id") is None:
            rollback_errors.append(f"{name}: changed table has no pre-run snapshot; empty-baseline rollback is unsupported.")
        if rollback:
            saved = rollback.get("tables", {}).get(name, {})
            if saved.get("target") != pin:
                common.append(f"{name}: rollback target differs from the original pin.")
            if head != pin and head != saved.get("before"):
                rollback_errors.append(f"{name}: unexpected head during rollback recovery.")
            if saved.get("status") == "complete" and head != pin:
                rollback_errors.append(f"{name}: previously restored table changed again.")
        elif head != pin and (not journal or journal.get("status") != "complete" or head != expected):
            rollback_errors.append(f"{name}: no unambiguous completed-write evidence.")
        if "failed" in statuses and head != pin and rollback:
            common.append(f"{name}: failed run no longer matches its verified rollback target.")
        report.append(entry)
    if state.get("arguments", {}).get("dry_run"):
        common.append("Dry runs have no reservation to recover.")
    identity = {"state": state, "completed": completed, "journals": journals,
                "heads": heads, "statuses": sorted(statuses), "rollback": rollback}
    return {
        "run_id": state.get("run_id"), "plan_sha256": digest(identity),
        "completed_stages": [stage for stage in STAGES if completed.get(stage)],
        "uncertain_tables": uncertain,
        "rollback_blockers": common + rollback_errors,
        "tables": report,
        "rollback_started": bool(rollback),
        "control_statuses": sorted(statuses),
        "caution": "Maintenance/exclusive-writer confirmation is mandatory; snapshot journals alone do not prove writer ownership.",
    }


def ancestor_path(current, target, snapshots):
    """Return the exact retained ancestry; fail on expiration or a different branch."""
    path, seen = [], set()
    while current != target:
        if current is None or current in seen or current not in snapshots:
            raise RecoveryBlocked("Pinned snapshot is unavailable or is not a retained ancestor.")
        seen.add(current)
        path.append(current)
        current = snapshots[current]
    if target is None or target not in snapshots:
        raise RecoveryBlocked("Cannot roll back to a missing/empty baseline snapshot.")
    return path


def rollback_publish(backend, state, plan):
    """Journal and restore publish changes in reverse order.

    Backend must be called under the recovery lease and operator maintenance.
    Preflight ALL tables/ancestors before the first rollback. This function
    deliberately never changes ``gold_run_control``. The separate control
    finalizer may close the reservation only after it independently verifies
    this journal and all five table heads.
    """
    if plan["rollback_blockers"]:
        raise RecoveryBlocked("; ".join(plan["rollback_blockers"]))
    saved = backend.get_rollback()
    if saved is None:
        saved = {"run_id": state["run_id"], "state_sha256": digest(state),
                 "status": "rollback_in_progress", "tables": {
                     row["name"]: {"before": row["current"], "target": row["pin"], "status": "pending"}
                     for row in plan["tables"]}}
        backend.put_rollback(saved)  # Durable before any table pointer change.
    for name in TABLES:
        item = saved["tables"][name]
        head = backend.head(name)
        if head != item["target"] and head != item["before"]:
            raise RecoveryBlocked(f"{name}: head changed after inspection.")
        if head != item["target"]:
            ancestor_path(head["snapshot_id"], item["target"]["snapshot_id"], backend.snapshots(name))
    for name in reversed(TABLES):
        item = saved["tables"][name]
        head = backend.head(name)
        if head != item["target"]:
            if head != item["before"]:
                raise RecoveryBlocked(f"{name}: concurrent change; rollback stopped.")
            # Recheck the original reservation before every pointer mutation.
            backend.assert_unfinished()
            item["status"] = "restoring"
            backend.put_rollback(saved)
            backend.rollback(name, item["target"]["snapshot_id"])
            if backend.head(name) != item["target"]:
                raise RecoveryBlocked(f"{name}: rollback verification failed.")
        item["status"] = "complete"
        backend.put_rollback(saved)
    if any(backend.head(n) != saved["tables"][n]["target"] for n in TABLES):
        raise RecoveryBlocked("Final five-table verification failed; control remains open.")
    saved["status"] = "tables_restored"
    backend.put_rollback(saved)
    return saved


def verify_all_pinned(backend, state):
    """Return current heads only when every Gold table matches its run pin."""
    pins = state.get("pins", {}).get("sinks", {})
    if set(pins) != set(TABLES):
        raise RecoveryBlocked("The run must pin exactly the five supported Gold tables.")
    heads = {name: backend.head(name) for name in TABLES}
    changed = [name for name in TABLES if heads[name] != pins[name]]
    if changed:
        raise RecoveryBlocked(
            "Control finalization is forbidden until all Gold tables match "
            f"their pinned pre-run snapshots; mismatches: {changed}"
        )
    return heads
