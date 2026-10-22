"""No AWS/Spark imports: exercise the destructive boundary with a fake catalog."""
import copy
import io
import unittest
from types import SimpleNamespace

from gold_recovery.plan import (
    STAGES, TABLES, RecoveryBlocked, ancestor_path, build_plan,
    rollback_publish, verify_all_pinned,
)
from gold_recovery.finalize_cli import (
    _require_argument_build_failure_state, _require_verified_publish_rollback,
)
from gold_recovery.runtime import (
    GoldBackend, assert_current_workflow_cluster, assert_failed_gold_step,
    assert_gold_step_not_submitted, assert_prior_cluster_stopped,
    _active_gold_yarn_applications, recovery_lease, verify_recovery_execution,
)


def evidence(count=0, writing=None):
    pins = {n: {"table": "glue_catalog.gold." + n, "missing": False, "snapshot_id": 100+i} for i,n in enumerate(TABLES)}
    state = {"run_id": "gold-test", "pins": {"sinks": pins}, "arguments": {"dry_run": False},
             "code_sha256": "original", "config_sha256": "original-config"}
    heads, journals = copy.deepcopy(pins), {}
    for n in TABLES[:count]:
        heads[n]["snapshot_id"] += 1000
        journals[n] = {"status": "complete", "head_after": copy.deepcopy(heads[n])}
    if writing:
        journals[writing] = {"status": "writing", "stats": {}}
    completed = {s: {"passed": True, "marker": True} for s in STAGES[:-1]}
    return state, completed, journals, heads


class Backend:
    def __init__(self, state, heads):
        self.state, self.heads = copy.deepcopy(state), copy.deepcopy(heads)
        self.saved, self.calls, self.closed = None, [], 0
        self.fail_name = None
        self.interrupt_after_commit = False
        self.corrupt_verify = False
        self.terminal = False
        self.history = {}
        for name in TABLES:
            base = state["pins"]["sinks"][name]["snapshot_id"]
            current = heads[name]["snapshot_id"]
            self.history[name] = {base: None, current: base if base != current else None}

    def head(self, name):
        return copy.deepcopy(self.heads[name])

    def snapshots(self, name):
        return self.history[name]

    def get_rollback(self):
        return copy.deepcopy(self.saved)

    def put_rollback(self, value):
        self.saved = copy.deepcopy(value)

    def assert_unfinished(self):
        if self.terminal:
            raise RecoveryBlocked("closed")

    def rollback(self, name, snapshot):
        self.calls.append(name)
        if name == self.fail_name and not self.interrupt_after_commit:
            raise RuntimeError("simulated cluster loss")
        if not self.corrupt_verify:
            self.heads[name]["snapshot_id"] = snapshot
        if name == self.fail_name and self.interrupt_after_commit:
            raise RuntimeError("lost rollback reply")

    def close_failed(self):
        self.closed += 1
        self.terminal = True


class RecoveryTests(unittest.TestCase):
    def test_plan_keeps_original_identity(self):
        s,c,j,h = evidence()
        p = build_plan(s,c,j,h,{"running"})
        self.assertFalse(p["rollback_blockers"])
        self.assertEqual(s["code_sha256"], "original")

    def test_noncontiguous_stage_markers_block_rollback(self):
        s,c,j,h = evidence()
        c = {"prepare": c["prepare"], "validate": c["validate"]}
        self.assertTrue(build_plan(s,c,j,h,{"running"})["rollback_blockers"])

    def test_partial_publish_skips_exact_journaled_heads(self):
        s,c,j,h = evidence(3)
        p = build_plan(s,c,j,h,{"running"})
        self.assertFalse(p["rollback_blockers"])
        self.assertEqual([r["classification"] for r in p["tables"][:3]], ["known_publish"]*3)

    def test_external_writer_blocks_rollback(self):
        s,c,j,h = evidence(3)
        h[TABLES[0]]["snapshot_id"] += 1
        p = build_plan(s,c,j,h,{"running"})
        self.assertTrue(p["rollback_blockers"])

    def test_uncertain_write_blocks_automatic_rollback(self):
        s,c,j,h = evidence(2, TABLES[2])
        h[TABLES[2]]["snapshot_id"] += 500
        p = build_plan(s,c,j,h,{"running"})
        self.assertEqual(p["uncertain_tables"], [TABLES[2]])
        self.assertTrue(p["rollback_blockers"])

    def test_writing_intent_without_commit_can_be_abandoned(self):
        s,c,j,h = evidence(0, TABLES[0])
        self.assertFalse(build_plan(s,c,j,h,{"running"})["rollback_blockers"])

    def test_reverse_rollback_leaves_control_open(self):
        s,c,j,h = evidence(5)
        b = Backend(s,h)
        rollback_publish(b,s,build_plan(s,c,j,h,{"running"}))
        self.assertEqual(b.calls,list(reversed(TABLES)))
        self.assertEqual(b.closed,0)
        self.assertEqual(b.saved["status"],"tables_restored")
        self.assertEqual(b.heads,s["pins"]["sinks"])
        self.assertEqual(verify_all_pinned(b,s),s["pins"]["sinks"])

    def test_publish_finalizer_requires_successful_matching_rollback(self):
        s,c,j,h = evidence(5)
        b = Backend(s,h)
        b.run_id = s["run_id"]
        with self.assertRaises(RecoveryBlocked):
            _require_verified_publish_rollback(b)
        rollback_publish(b,s,build_plan(s,c,j,h,{"running"}))
        _require_verified_publish_rollback(b)
        b.saved["status"] = "rollback_in_progress"
        with self.assertRaises(RecoveryBlocked):
            _require_verified_publish_rollback(b)

    def test_control_finalizer_head_verification_rejects_any_change(self):
        s,c,j,h = evidence()
        b = Backend(s,h)
        b.heads[TABLES[0]]["snapshot_id"] += 1
        with self.assertRaises(RecoveryBlocked):
            verify_all_pinned(b,s)

    def test_failed_rollback_does_not_close_control(self):
        s,c,j,h = evidence(5)
        b = Backend(s,h); b.fail_name=TABLES[2]
        with self.assertRaises(RuntimeError):
            rollback_publish(b,s,build_plan(s,c,j,h,{"running"}))
        self.assertEqual(b.closed,0)
        self.assertEqual(b.saved["status"],"rollback_in_progress")

    def test_retry_skips_restored_tables(self):
        s,c,j,h = evidence(5)
        b=Backend(s,h); b.fail_name=TABLES[2]
        with self.assertRaises(RuntimeError):
            rollback_publish(b,s,build_plan(s,c,j,h,{"running"}))
        first=list(b.calls); b.fail_name=None
        p=build_plan(s,c,j,b.heads,{"running"},b.saved)
        self.assertFalse(p["rollback_blockers"])
        rollback_publish(b,s,p)
        self.assertEqual(b.calls.count(TABLES[4]),1)
        self.assertEqual(b.calls.count(TABLES[3]),1)
        self.assertEqual(b.closed,0)

    def test_lost_rollback_reply_is_reconciled_by_exact_head(self):
        s,c,j,h = evidence(3)
        b=Backend(s,h); b.fail_name=TABLES[2]; b.interrupt_after_commit=True
        with self.assertRaises(RuntimeError):
            rollback_publish(b,s,build_plan(s,c,j,h,{"running"}))
        self.assertEqual(b.saved["tables"][TABLES[2]]["status"],"restoring")
        b.fail_name=None
        rollback_publish(b,s,build_plan(s,c,j,b.heads,{"running"},b.saved))
        self.assertEqual(b.calls.count(TABLES[2]),1)
        self.assertEqual(b.closed,0)

    def test_missing_ancestor_preflights_all_before_first_table_mutation(self):
        s,c,j,h=evidence(5)
        b=Backend(s,h); b.history[TABLES[0]]={}
        with self.assertRaises(RecoveryBlocked):
            rollback_publish(b,s,build_plan(s,c,j,h,{"running"}))
        self.assertEqual(b.calls,[])
        self.assertEqual(b.closed,0)

    def test_empty_first_build_is_not_blindly_rolled_back(self):
        s,c,j,h=evidence(1)
        s["pins"]["sinks"][TABLES[0]]["snapshot_id"]=None
        self.assertTrue(build_plan(s,c,j,h,{"running"})["rollback_blockers"])

    def test_unchanged_empty_tables_can_be_closed(self):
        s,c,j,h=evidence()
        for n in TABLES:
            s["pins"]["sinks"][n]["snapshot_id"]=None; h[n]["snapshot_id"]=None
        p=build_plan(s,c,j,h,{"running"})
        self.assertFalse(p["rollback_blockers"])

    def test_failed_dq_is_not_reused_as_success(self):
        s,c,j,h=evidence(); c["validate"]["passed"]=False
        self.assertTrue(build_plan(s,c,j,h,{"running"})["rollback_blockers"])

    def test_succeeded_and_manually_failed_runs_are_not_rolled_back(self):
        s,c,j,h=evidence()
        for statuses in ({"running","succeeded"},{"running","failed"}):
            self.assertTrue(build_plan(s,c,j,h,statuses)["rollback_blockers"])

    def test_completed_publish_marker_still_rolls_back_under_policy(self):
        s,c,j,h=evidence(5);c["publish"]={"marker":True}
        p=build_plan(s,c,j,h,{"running"})
        self.assertFalse(p["rollback_blockers"])

    def test_rollback_journal_allows_idempotent_completion(self):
        s,c,j,h=evidence(1);b=Backend(s,h);b.fail_name=TABLES[0]
        with self.assertRaises(RuntimeError):
            rollback_publish(b,s,build_plan(s,c,j,h,{"running"}))
        self.assertFalse(build_plan(s,c,j,b.heads,{"running"},b.saved)["rollback_blockers"])

    def test_unrelated_change_after_partial_rollback_blocks_continuation(self):
        s,c,j,h=evidence(3);b=Backend(s,h);b.fail_name=TABLES[1]
        with self.assertRaises(RuntimeError):
            rollback_publish(b,s,build_plan(s,c,j,h,{"running"}))
        b.heads[TABLES[2]]["snapshot_id"] += 17
        self.assertTrue(build_plan(s,c,j,b.heads,{"running"},b.saved)["rollback_blockers"])

    def test_bad_verification_never_closes_control(self):
        s,c,j,h=evidence(1);b=Backend(s,h);b.corrupt_verify=True
        with self.assertRaises(RecoveryBlocked):
            rollback_publish(b,s,build_plan(s,c,j,h,{"running"}))
        self.assertEqual(b.closed,0)

    def test_ancestry_requires_retained_pin_and_no_cycles(self):
        self.assertEqual(ancestor_path(3,1,{3:2,2:1,1:None}),[3,2])
        for history in ({3:2,2:None,1:None},{3:3,1:None},{3:1}):
            with self.assertRaises(RecoveryBlocked):
                ancestor_path(3,1,history)

    def test_inspection_digest_changes_if_head_changes(self):
        s,c,j,h=evidence()
        a=build_plan(s,c,j,h,{"running"})["plan_sha256"]
        h[TABLES[0]]["snapshot_id"] += 1
        self.assertNotEqual(a,build_plan(s,c,j,h,{"running"})["plan_sha256"])

    def test_old_cluster_must_really_be_terminated(self):
        class Emr:
            def __init__(self,status): self.status=status
            def describe_cluster(self,ClusterId):return {"Cluster":{"Id":ClusterId,"Status":{"State":self.status},"Tags":[{"Key":"lotus-workflow","Value":"silver-gold-transient"}]}}
        self.assertEqual(assert_prior_cluster_stopped(Emr("TERMINATED"),"j-abc")["state"],"TERMINATED")
        with self.assertRaises(RecoveryBlocked):assert_prior_cluster_stopped(Emr("WAITING"),"j-abc")

    def test_automatic_recovery_requires_active_tagged_workflow_cluster(self):
        class Emr:
            def __init__(self, state="WAITING", tag="silver-gold-transient"):
                self.state, self.tag = state, tag
            def describe_cluster(self, ClusterId):
                return {"Cluster": {"Id": ClusterId, "Status": {"State": self.state},
                                    "Tags": [{"Key": "lotus-workflow", "Value": self.tag}]}}
        result = assert_current_workflow_cluster(Emr(), "j-abc")
        self.assertEqual(result["workflow"], "silver-gold-transient")
        for client in (Emr("TERMINATED"), Emr(tag="unrelated")):
            with self.assertRaises(RecoveryBlocked):
                assert_current_workflow_cluster(client, "j-abc")

    def test_automatic_recovery_requires_terminal_failed_named_step(self):
        class Emr:
            def __init__(self, states):self.states=states
            def list_steps(self, **kwargs):
                return {"Steps": [{"Name": "gold-publish-gold-test",
                                    "Status": {"State": state}}
                                   for state in self.states]}
        proof = assert_failed_gold_step(Emr(["FAILED", "FAILED"]), "j-abc",
                                        "gold-test", "publish")
        self.assertEqual(proof["attempts"], 2)
        for states in (["RUNNING", "FAILED"], ["COMPLETED"]):
            with self.assertRaises(RecoveryBlocked):
                assert_failed_gold_step(Emr(states), "j-abc", "gold-test", "publish")

    def test_argument_failure_requires_no_submitted_step(self):
        class Emr:
            def __init__(self, steps): self.steps = steps
            def list_steps(self, **kwargs): return {"Steps": self.steps}
        proof = assert_gold_step_not_submitted(
            Emr([]), "j-abc", "gold-test", "accounts")
        self.assertTrue(proof["not_submitted"])
        with self.assertRaises(RecoveryBlocked):
            assert_gold_step_not_submitted(
                Emr([{"Name": "gold-accounts-gold-test", "Status": {"State": "FAILED"}}]),
                "j-abc", "gold-test", "accounts")

    def test_replacement_compute_separates_failed_and_recovery_clusters(self):
        class Emr:
            def describe_cluster(self, ClusterId):
                state = "TERMINATED" if ClusterId == "j-failed" else "WAITING"
                return {"Cluster": {"Id": ClusterId, "Status": {"State": state},
                                    "Tags": [{"Key": "lotus-workflow", "Value": "silver-gold-transient"}]}}
            def list_steps(self, **kwargs):
                return {"Steps": [{"Name": "gold-publish-gold-test",
                                    "Status": {"State": "FAILED"}}]}
        proof = verify_recovery_execution(
            Emr(), failed_cluster_id="j-failed",
            recovery_cluster_id="j-recovery", run_id="gold-test",
            failed_stage="publish", failure_kind="spark_step")
        self.assertTrue(proof["replacement_compute"])

    def test_active_normal_gold_yarn_application_is_detected(self):
        class Report:
            def __init__(self, app_id, name, state):
                self.values = app_id, name, state
            def getApplicationId(self): return self.values[0]
            def getName(self): return self.values[1]
            def getYarnApplicationState(self): return self.values[2]
        reports = [
            Report("application_1", "lotus_gold.gold_account_customer_map.publish", "RUNNING"),
            Report("application_2", "lotus_gold.gold_publish_rollback", "RUNNING"),
            Report("application_3", "lotus_gold.gold_account_customer_map.accounts", "FAILED"),
        ]
        active = _active_gold_yarn_applications(reports, "lotus_gold")
        self.assertEqual([item["application_id"] for item in active], ["application_1"])

    def test_identity_errors_are_enforced_before_mutation(self):
        backend = GoldBackend.__new__(GoldBackend)
        backend.identity_errors = ["code mismatch", "config mismatch"]
        with self.assertRaises(RecoveryBlocked):
            backend.assert_identity()
        backend.identity_errors = []
        backend.assert_identity()

    def test_argument_failure_requires_predecessor_and_no_publish_journal(self):
        class Store:
            def __init__(self): self.values = {"completed/prepare": {"marker": True}}
            def get(self, name): return self.values.get(name)
        backend = SimpleNamespace(store=Store())
        _require_argument_build_failure_state(backend, "accounts")
        backend.store.values["publish/account_changes_canonical_events"] = {"status": "writing"}
        with self.assertRaises(RecoveryBlocked):
            _require_argument_build_failure_state(backend, "accounts")

    def test_recovery_lease_is_conditional_and_released_by_its_owner(self):
        class S3:
            def __init__(self):self.data={};self.deleted=[]
            def put_object(self,**kw):
                self.assertion=kw["IfNoneMatch"]
                if kw["Key"] in self.data:raise RuntimeError("PreconditionFailed")
                self.data[kw["Key"]]=kw["Body"]
            def get_object(self,**kw):return {"Body":io.BytesIO(self.data[kw["Key"]])}
            def delete_object(self,**kw):self.deleted.append(kw["Key"]);del self.data[kw["Key"]]
        s3=S3();store=SimpleNamespace(client=s3,bucket="bucket",key="staging/run_id=gold-test")
        with recovery_lease(store,"gold-test"):
            self.assertIn("staging/_gold_recovery.lock.json",s3.data)
            with self.assertRaises(RecoveryBlocked):
                with recovery_lease(store,"other"):pass
        self.assertEqual(s3.assertion,"*")
        self.assertEqual(s3.deleted,["staging/_gold_recovery.lock.json"])

    def test_lease_owner_change_does_not_delete_someone_elses_lock(self):
        class S3:
            def put_object(self,**kwargs):pass
            def get_object(self,**kwargs):return {"Body":io.BytesIO(b'{"owner":"different"}')}
            def delete_object(self,**kwargs):raise AssertionError("Must not delete")
        store=SimpleNamespace(client=S3(),bucket="bucket",key="staging/run_id=x")
        with self.assertRaises(RecoveryBlocked):
            with recovery_lease(store,"x"):pass


if __name__ == "__main__":
    unittest.main()
