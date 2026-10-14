from dataclasses import FrozenInstanceError
import unittest

from mas_host.acceptance_policies import (AcceptanceDecision, AcceptanceError, AggregatePolicy,
    AssessmentSubject, DesignatedReviewerPolicy, DeterministicPolicy)


class AcceptancePolicyTests(unittest.TestCase):
    def setUp(self):
        self.subject = AssessmentSubject({"revision": "abc", "value": 2}, {"value": 4})
        self.yes = DeterministicPolicy("double", "1", lambda i, o: o["value"] == 2 * i["value"])
        self.no = DeterministicPolicy("different", "1", lambda i, o: False)
        self.unknown = DeterministicPolicy("pending", "1", lambda i, o: None)

    def test_subject_tracks_both_input_and_output_and_is_copy_protected(self):
        data = {"revision": "abc", "value": 2}
        subject = AssessmentSubject(data, {"value": 4})
        identity = subject.identity
        data["revision"] = "def"
        subject.input["revision"] = "xyz"
        subject.output["value"] = 5
        self.assertEqual(subject.identity, identity)
        self.assertNotEqual(identity, AssessmentSubject(data, {"value": 4}).identity)
        self.assertNotEqual(identity, AssessmentSubject(subject.input, {"value": 5}).identity)
        # Object key order is not a different input.
        self.assertEqual(identity, AssessmentSubject({"value": 2, "revision": "abc"}, {"value": 4}).identity)

    def test_deterministic_boolean_none_and_failures_have_explicit_meanings(self):
        self.assertEqual(self.yes.evaluate(self.subject).status, "accepted")
        self.assertEqual(self.no.evaluate(self.subject).status, "rejected")
        self.assertEqual(self.unknown.evaluate(self.subject).status, "unresolved")
        invalid = DeterministicPolicy("invalid", "1", lambda i, o: 1)
        self.assertEqual(invalid.evaluate(self.subject).reason, "invalid_validator_result")
        def fail(i, o):
            raise RuntimeError("sensitive source material")
        decision = DeterministicPolicy("fails", "1", fail).evaluate(self.subject)
        self.assertEqual(decision.reason, "validator_failed")
        self.assertNotIn("sensitive", str(decision.descriptor))

    def test_mutating_validator_receives_snapshots(self):
        def mutate(i, o):
            i.clear()
            o.clear()
            return True
        policy = DeterministicPolicy("mutating", "1", mutate)
        identity = self.subject.identity
        policy.evaluate(self.subject)
        self.assertEqual(self.subject.identity, identity)

    def test_human_decision_requires_authenticated_external_identity(self):
        policy = DesignatedReviewerPolicy("owner-review", "1", "alice@example.com")
        payload = {"status": "accepted", "subject_identity": self.subject.identity,
                   "reason": "reviewed", "evidence_ids": ["review-1"]}
        with self.assertRaisesRegex(AcceptanceError, "reviewer forbidden"):
            policy.evaluate(self.subject, authenticated_reviewer="mallory", decision=payload)
        with self.assertRaisesRegex(AcceptanceError, "invalid reviewer decision"):
            policy.evaluate(self.subject, authenticated_reviewer="alice@example.com",
                            decision=payload | {"reviewer": "alice@example.com"})
        decision = policy.evaluate(self.subject, authenticated_reviewer="alice@example.com", decision=payload)
        payload["evidence_ids"].append("later")
        self.assertEqual(decision.evidence_ids, ("review-1",))
        self.assertEqual(decision.status, "accepted")
        self.assertEqual(decision.policy_identity, policy.identity)

    def test_stale_human_decision_cannot_accept_changed_input_or_output(self):
        policy = DesignatedReviewerPolicy("review", "1", "alice")
        payload = {"status": "accepted", "subject_identity": self.subject.identity,
                   "reason": "reviewed", "evidence_ids": []}
        changed = AssessmentSubject({"revision": "def", "value": 2}, {"value": 4})
        decision = policy.evaluate(changed, authenticated_reviewer="alice", decision=payload)
        self.assertEqual(decision.status, "unresolved")
        self.assertEqual(decision.subject_identity, changed.identity)

    def test_all_of_requires_every_declared_policy_and_rejection_blocks(self):
        policy = AggregatePolicy("all-checks", "1", [self.yes.identity, self.no.identity])
        self.assertEqual(policy.evaluate(self.subject, []).status, "unresolved")
        self.assertEqual(policy.evaluate(self.subject, [self.yes.evaluate(self.subject)]).status, "unresolved")
        self.assertEqual(policy.evaluate(self.subject, [self.no.evaluate(self.subject)]).status, "rejected")
        second_yes = DeterministicPolicy("other", "1", lambda i, o: True)
        success = AggregatePolicy("all-checks", "1", [self.yes.identity, second_yes.identity])
        self.assertEqual(success.evaluate(self.subject, [self.yes.evaluate(self.subject),
            second_yes.evaluate(self.subject)]).status, "accepted")

    def test_quorum_distinguishes_missing_evidence_from_unreachable_threshold(self):
        policy = AggregatePolicy("vote", "1", [self.yes.identity, self.no.identity, self.unknown.identity],
                                 mode="quorum", threshold=2)
        values = [self.yes.evaluate(self.subject), self.no.evaluate(self.subject), self.unknown.evaluate(self.subject)]
        self.assertEqual(policy.evaluate(self.subject, values).status, "unresolved")
        values[2] = AcceptanceDecision("accepted", self.subject.identity, self.unknown.identity, "resolved")
        self.assertEqual(policy.evaluate(self.subject, values).status, "accepted")
        values[2] = AcceptanceDecision("rejected", self.subject.identity, self.unknown.identity, "resolved")
        self.assertEqual(policy.evaluate(self.subject, values).status, "rejected")

    def test_duplicate_foreign_or_stale_decisions_do_not_count_as_votes(self):
        policy = AggregatePolicy("checks", "1", [self.yes.identity])
        accepted = self.yes.evaluate(self.subject)
        with self.assertRaisesRegex(AcceptanceError, "duplicate"):
            policy.evaluate(self.subject, [accepted, accepted])
        with self.assertRaisesRegex(AcceptanceError, "unregistered"):
            policy.evaluate(self.subject, [self.no.evaluate(self.subject)])
        changed = AssessmentSubject({"revision": "changed"}, {})
        stale = policy.evaluate(changed, [accepted])
        self.assertEqual(stale.status, "unresolved")
        self.assertEqual(stale.reason, "stale_acceptance_evidence")

    def test_aggregate_policy_order_does_not_change_identity(self):
        a = AggregatePolicy("checks", "1", [self.yes.identity, self.no.identity])
        b = AggregatePolicy("checks", "1", [self.no.identity, self.yes.identity])
        self.assertEqual(a.identity, b.identity)
        a.descriptor["policies"].clear()
        self.assertEqual(a.identity, b.identity)
        with self.assertRaises(FrozenInstanceError):
            a.threshold = 1
        first = a.evaluate(self.subject, [self.yes.evaluate(self.subject), self.no.evaluate(self.subject)])
        second = a.evaluate(self.subject, [self.no.evaluate(self.subject), self.yes.evaluate(self.subject)])
        self.assertEqual(first, second)

    def test_invalid_policy_and_subject_configuration_fail_closed(self):
        for threshold in (True, 0, 2, None):
            with self.subTest(threshold=threshold), self.assertRaises(AcceptanceError):
                AggregatePolicy("q", "1", [self.yes.identity], mode="quorum", threshold=threshold)
        for input in ([], {"float": 1.2}):
            with self.assertRaises(AcceptanceError):
                AssessmentSubject(input, {})
        with self.assertRaises(AcceptanceError):
            AggregatePolicy("q", "1", [self.yes.identity, self.yes.identity])
        with self.assertRaises(AcceptanceError):
            AcceptanceDecision("completed", self.subject.identity, self.yes.identity, "done")


if __name__ == "__main__":
    unittest.main()
