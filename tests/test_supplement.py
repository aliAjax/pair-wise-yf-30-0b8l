import sys
import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, PharmacovigilanceService, iso

UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)
DAY = timedelta(days=1)


class SupplementPauseFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.svc = PharmacovigilanceService(Path(self.tmp.name) / "test.db")

    def tearDown(self):
        self.tmp.cleanup()

    def _case(self, dedupe="intake-1", serious=False, fatal=False, received=T0):
        return self.svc.create_case(
            "reporter-a", "reporter", "CN",
            {"patient_ref": "P-1", "region": "CN", "product": "DrugA", "event_term": "肝损伤",
             "source": "email", "dedupe_key": dedupe, "received_at": iso(received),
             "serious": serious, "fatal": fatal},
        )["case"]

    def _review(self, case, serious, fatal, expected, received=T0):
        return self.svc.medical_review(
            case["id"], "reviewer-1", "medical_reviewer",
            {"expected_revision": expected, "serious": serious, "fatal": fatal,
             "causality": "possibly_related", "rationale": "已核验", "received_at": iso(received)},
        )

    def test_pause_then_resume_keeps_remaining_budget(self):
        case = self._case()
        report = self.svc.create_report(case["id"], "lead-cn", "regional_lead", "CN", {"country": "CN"})
        # 暂停前：非严重 90 天，到期 T0+90
        self.assertEqual(report["due_at"], iso(T0 + 90 * DAY))

        paused = self.svc.request_supplement(
            report["id"], "lead-cn", "regional_lead", "CN", {"requested_at": iso(T0 + 10 * DAY)})
        self.assertEqual(paused["report"]["status"], "paused")
        self.assertEqual(paused["supplement"]["remaining_seconds"], 80 * 86400)

        fulfilled = self.svc.fulfill_supplement(
            paused["supplement"]["id"], "lead-cn", "regional_lead", "CN",
            {"expected_revision": 1, "fulfilled_at": iso(T0 + 30 * DAY)})
        # 恢复时到期 = 到齐时刻 + 暂停时剩余 80 天
        self.assertEqual(fulfilled["report"]["status"], "pending")
        self.assertEqual(fulfilled["report"]["due_at"], iso(T0 + 110 * DAY))

    def test_severity_change_during_pause_recalculates_remaining(self):
        case = self._case()
        cn = self.svc.create_report(case["id"], "lead-cn", "regional_lead", "CN", {"country": "CN"})
        us = self.svc.create_report(case["id"], "lead-cn", "regional_lead", "CN", {"country": "US"})

        paused = self.svc.request_supplement(
            cn["id"], "lead-cn", "regional_lead", "CN", {"requested_at": iso(T0 + 10 * DAY)})
        self.assertEqual(paused["supplement"]["remaining_seconds"], 80 * 86400)

        # 暂停期间严重性改为严重（15 天规则）：剩余 = 新到期(T0+15) - 暂停时刻(T0+10) = 5 天
        self._review(case, True, False, expected=1)
        detail = self.svc.get_case(case["id"], "global_admin", "")
        supp = detail["supplements"][0]
        self.assertEqual(supp["remaining_seconds"], 5 * 86400)
        # 其他国家报告不受影响
        us_after = next(r for r in detail["reports"] if r["country"] == "US")
        self.assertEqual(us_after["due_at"], iso(T0 + 90 * DAY))
        self.assertEqual(us_after["status"], "pending")
        # 该国报告仍暂停，到期日未被改动
        cn_after = next(r for r in detail["reports"] if r["country"] == "CN")
        self.assertEqual(cn_after["status"], "paused")
        self.assertEqual(cn_after["due_at"], iso(T0 + 90 * DAY))

        fulfilled = self.svc.fulfill_supplement(
            supp["id"], "lead-cn", "regional_lead", "CN",
            {"expected_revision": supp["revision"], "fulfilled_at": iso(T0 + 30 * DAY)})
        self.assertEqual(fulfilled["report"]["due_at"], iso(T0 + 35 * DAY))

    def test_fatal_severity_during_pause_recalculates_to_negative(self):
        case = self._case()
        report = self.svc.create_report(case["id"], "lead-cn", "regional_lead", "CN", {"country": "CN"})
        paused = self.svc.request_supplement(
            report["id"], "lead-cn", "regional_lead", "CN", {"requested_at": iso(T0 + 10 * DAY)})
        # 暂停期间改为致死（7 天规则）：新到期 T0+7 早于暂停时刻 T0+10，剩余为负
        self._review(case, True, True, expected=1)
        detail = self.svc.get_case(case["id"], "global_admin", "")
        supp = detail["supplements"][0]
        self.assertEqual(supp["remaining_seconds"], -3 * 86400)
        fulfilled = self.svc.fulfill_supplement(
            supp["id"], "lead-cn", "regional_lead", "CN",
            {"expected_revision": supp["revision"], "fulfilled_at": iso(T0 + 30 * DAY)})
        # 恢复即逾期
        self.assertEqual(fulfilled["report"]["due_at"], iso(T0 + 27 * DAY))

    def test_late_fulfillment_gets_conflict(self):
        case = self._case()
        report = self.svc.create_report(case["id"], "lead-cn", "regional_lead", "CN", {"country": "CN"})
        paused = self.svc.request_supplement(
            report["id"], "lead-cn", "regional_lead", "CN", {"requested_at": iso(T0 + 10 * DAY)})
        supp_id = paused["supplement"]["id"]

        first = self.svc.fulfill_supplement(
            supp_id, "lead-cn", "regional_lead", "CN",
            {"expected_revision": 1, "fulfilled_at": iso(T0 + 30 * DAY)})
        self.assertEqual(first["supplement"]["status"], "fulfilled")

        # 后到的提交携带旧版本，收到冲突提示
        with self.assertRaises(ApiError) as ctx:
            self.svc.fulfill_supplement(
                supp_id, "lead-cn", "regional_lead", "CN",
                {"expected_revision": 1, "fulfilled_at": iso(T0 + 31 * DAY)})
        self.assertEqual(ctx.exception.status, 409)
        self.assertEqual(ctx.exception.code, "revision_conflict")

    def test_wrong_revision_conflict_preserves_timer_and_can_retry(self):
        case = self._case()
        report = self.svc.create_report(case["id"], "lead-cn", "regional_lead", "CN", {"country": "CN"})
        paused = self.svc.request_supplement(
            report["id"], "lead-cn", "regional_lead", "CN", {"requested_at": iso(T0 + 10 * DAY)})
        supp_id = paused["supplement"]["id"]

        # 重算携带错误版本：失败但原计时保留
        with self.assertRaises(ApiError) as ctx:
            self.svc.recalculate_supplement(
                supp_id, "lead-cn", "regional_lead", "CN", {"expected_revision": 99})
        self.assertEqual(ctx.exception.code, "revision_conflict")
        detail = self.svc.get_case(case["id"], "global_admin", "")
        self.assertEqual(detail["supplements"][0]["remaining_seconds"], 80 * 86400)
        self.assertEqual(detail["supplements"][0]["revision"], 1)

        # 重试成功
        rec = self.svc.recalculate_supplement(
            supp_id, "lead-cn", "regional_lead", "CN", {"expected_revision": 1})
        self.assertEqual(rec["supplement"]["revision"], 2)

    def test_paused_report_excluded_from_overdue_and_visible_in_state(self):
        # 建一个已逾期的非严重案例（接收于 100 天前，90 天期限已过）
        case = self._case(received=datetime.now(UTC) - 100 * DAY)
        report = self.svc.create_report(case["id"], "lead-cn", "regional_lead", "CN", {"country": "CN"})

        overdue_before = self.svc.overdue("global_admin", "")
        self.assertTrue(any(r["id"] == report["id"] for r in overdue_before))

        paused = self.svc.request_supplement(report["id"], "lead-cn", "regional_lead", "CN", {})
        self.assertEqual(paused["report"]["status"], "paused")

        # 暂停后逾期清单不再包含，且状态可见
        self.assertFalse(any(r["id"] == report["id"] for r in self.svc.overdue("global_admin", "")))
        state = self.svc.state("global_admin", "")
        state_report = next(r for r in state["reports"] if r["id"] == report["id"])
        self.assertEqual(state_report["status"], "paused")

        # 资料到齐恢复：剩余为负，恢复即逾期
        fulfilled = self.svc.fulfill_supplement(
            paused["supplement"]["id"], "lead-cn", "regional_lead", "CN", {"expected_revision": 1})
        self.assertEqual(fulfilled["report"]["status"], "pending")
        self.assertTrue(any(r["id"] == report["id"] for r in self.svc.overdue("global_admin", "")))

    def test_cannot_pause_submitted_or_double_pause(self):
        case = self._case()
        report = self.svc.create_report(case["id"], "lead-cn", "regional_lead", "CN", {"country": "CN"})
        self.svc.submit_report(report["id"], "lead-cn", "regional_lead", "CN", {})
        with self.assertRaises(ApiError) as ctx:
            self.svc.request_supplement(report["id"], "lead-cn", "regional_lead", "CN", {})
        self.assertEqual(ctx.exception.code, "report_submitted")

        case2 = self._case("intake-2")
        report2 = self.svc.create_report(case2["id"], "lead-cn", "regional_lead", "CN", {"country": "CN"})
        self.svc.request_supplement(report2["id"], "lead-cn", "regional_lead", "CN", {})
        with self.assertRaises(ApiError) as ctx:
            self.svc.request_supplement(report2["id"], "lead-cn", "regional_lead", "CN", {})
        self.assertEqual(ctx.exception.code, "supplement_already_open")


if __name__ == "__main__":
    unittest.main()
