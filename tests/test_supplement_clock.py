import sys
import tempfile
import threading
import unittest
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, PharmacovigilanceService, iso, parse_time, utcnow


class SupplementClockTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.svc = PharmacovigilanceService(Path(self.tmp.name) / "test.db")
        self.t0 = utcnow().replace(microsecond=0)
        self.case = self.svc.create_case(
            "reporter-a", "reporter", "CN",
            {"patient_ref": "P-1", "region": "CN", "product": "DrugA", "event_term": "肝损伤",
             "source": "email", "dedupe_key": "sup-1", "received_at": iso(self.t0), "serious": False},
        )["case"]
        self.report_cn = self.svc.create_report(
            self.case["id"], "lead-cn", "regional_lead", "CN", {"country": "CN"})
        self.report_us = self.svc.create_report(
            self.case["id"], "lead-cn", "regional_lead", "CN", {"country": "US"})

    def tearDown(self):
        self.tmp.cleanup()

    def due(self, report):
        return parse_time(report["due_at"])

    def test_pause_freezes_clock_and_resume_continues_remaining_budget(self):
        # 非严重案例窗口 90 天；第 10 天收到补件要求，剩余约 80 天
        pause_at = self.t0 + timedelta(days=10)
        paused = self.svc.request_supplement(
            self.report_cn["id"], "lead-cn", "regional_lead", "CN",
            {"authority": "NMPA", "requested_at": iso(pause_at), "documents": ["hospital_summary.pdf"]})
        self.assertEqual(paused["report"]["clock_status"], "paused")
        self.assertTrue(paused["report"]["paused"])
        self.assertAlmostEqual(paused["remaining_seconds"], 80 * 86400, delta=2)
        self.assertEqual(paused["report"]["remaining_seconds"], paused["remaining_seconds"])

        # 暂停期间即使真实时间走到第 150 天，也不逾期、时钟不动
        far_later = self.t0 + timedelta(days=150)
        overdue_ids = [r["id"] for r in self.svc.overdue("regional_lead", "CN")]
        self.assertNotIn(self.report_cn["id"], overdue_ids)
        paused_list = self.svc.paused("regional_lead", "CN")
        self.assertEqual([r["id"] for r in paused_list], [self.report_cn["id"]])

        # 资料到齐：按暂停时剩余天数续算，新到期 = 恢复时刻 + 剩余预算
        resume_at = far_later
        result = self.svc.submit_supplement(
            paused["supplement_request_id"], "reporter-a", "reporter", "CN",
            {"documents": ["hospital_summary.pdf", "lab.pdf"],
             "registered_at": iso(resume_at - timedelta(hours=2)),
             "resumed_at": iso(resume_at)})
        self.assertFalse(result["conflict"])
        self.assertEqual(result["report"]["clock_status"], "running")
        self.assertAlmostEqual(
            (self.due(result["report"]) - resume_at).total_seconds(), 80 * 86400, delta=2)
        # 恢复后才重新计时：到期前不逾期，到期后逾期
        self.assertEqual(self.svc.overdue("global_admin", ""), [])

    def test_pause_is_isolated_per_country(self):
        pause_at = self.t0 + timedelta(days=10)
        self.svc.request_supplement(
            self.report_cn["id"], "lead-cn", "regional_lead", "CN",
            {"authority": "NMPA", "requested_at": iso(pause_at)})
        cn = self.svc.get_case(self.case["id"], "global_admin", "")["reports"]
        by_country = {r["country"]: r for r in cn}
        self.assertEqual(by_country["CN"]["clock_status"], "paused")
        self.assertEqual(by_country["US"]["clock_status"], "running")
        # 美国时钟仍按原 90 天窗口走
        self.assertAlmostEqual(
            by_country["US"]["remaining_seconds"],
            (self.due(by_country["US"]) - utcnow()).total_seconds(), delta=5)

    def test_severity_change_during_pause_recalculates_remaining_days(self):
        # 第 20 天暂停 CN，非严重窗口剩 70 天
        pause_at = self.t0 + timedelta(days=20)
        paused = self.svc.request_supplement(
            self.report_cn["id"], "lead-cn", "regional_lead", "CN",
            {"authority": "NMPA", "requested_at": iso(pause_at)})
        self.assertAlmostEqual(paused["remaining_seconds"], 70 * 86400, delta=2)
        # 同一案例另一国 US 也暂停；用第二个案例验证其他案例完全不受影响
        other_case = self.svc.create_case(
            "reporter-a", "reporter", "CN",
            {"patient_ref": "P-2", "region": "CN", "product": "DrugB", "event_term": "皮疹",
             "source": "email", "dedupe_key": "sup-2", "received_at": iso(self.t0), "serious": False},
        )["case"]
        other_report = self.svc.create_report(
            other_case["id"], "lead-cn", "regional_lead", "CN", {"country": "CN"})
        other_paused = self.svc.request_supplement(
            other_report["id"], "lead-cn", "regional_lead", "CN",
            {"authority": "NMPA", "requested_at": iso(pause_at)})

        # 医学审核员在暂停期间裁定为严重（非死亡，15 天窗口）；received_at 仍为 t0
        self.svc.medical_review(
            self.case["id"], "reviewer-1", "medical_reviewer",
            {"expected_revision": 1, "serious": True, "fatal": False,
             "causality": "related", "rationale": "住院即严重", "received_at": iso(self.t0)})

        detail = {r["country"]: r for r in self.svc.get_case(self.case["id"], "global_admin", "")["reports"]}
        # 新窗口 = received_at + 15 天；暂停发生在第 20 天，剩余 = -5 天
        self.assertTrue(detail["CN"]["paused"])
        self.assertAlmostEqual(detail["CN"]["remaining_seconds"], -5 * 86400, delta=2)
        self.assertEqual(detail["CN"]["recalc_failed"], False)
        # 同案例运行中的 US 时钟不被重算，仍是原 90 天绝对到期
        self.assertEqual(detail["US"]["clock_status"], "running")
        # 其他案例的暂停时钟不受影响（仍剩 70 天）
        other_view = self.svc.paused("regional_lead", "CN")
        other_row = next(r for r in other_view if r["id"] == other_report["id"])
        self.assertAlmostEqual(other_row["remaining_seconds"], 70 * 86400, delta=2)

        # 补齐恢复时按重算后的预算走（负值 => 恢复即逾期）
        result = self.svc.submit_supplement(
            paused["supplement_request_id"], "reporter-a", "reporter", "CN",
            {"documents": ["docs.pdf"], "registered_at": iso(pause_at + timedelta(days=1)),
             "resumed_at": iso(pause_at + timedelta(days=2))})
        self.assertAlmostEqual(
            (self.due(result["report"]) - (pause_at + timedelta(days=2))).total_seconds(),
            -5 * 86400, delta=2)

    def test_duplicate_supplement_earliest_registered_wins(self):
        pause_at = self.t0 + timedelta(days=30)
        paused = self.svc.request_supplement(
            self.report_cn["id"], "lead-cn", "regional_lead", "CN",
            {"authority": "NMPA", "requested_at": iso(pause_at)})
        sid = paused["supplement_request_id"]
        # A 先到但登记时间晚；B 后到但登记时间更早 -> B 收到冲突提示，但最终被采用
        win = self.svc.submit_supplement(
            sid, "reporter-a", "reporter", "CN",
            {"documents": ["a.pdf"], "registered_at": iso(pause_at + timedelta(days=3))})
        self.assertEqual(win["supplement"]["submitted_by"], "reporter-a")
        with self.assertRaises(ApiError) as ctx:
            self.svc.submit_supplement(
                sid, "reporter-b", "reporter", "CN",
                {"documents": ["b.pdf"], "registered_at": iso(pause_at + timedelta(days=1))})
        self.assertEqual(ctx.exception.code, "supplement_conflict")
        req = self.svc.repo.conn.execute(
            "SELECT submitted_by FROM supplement_requests WHERE id=?", (sid,)).fetchone()
        self.assertEqual(req["submitted_by"], "reporter-b")

    def test_duplicate_supplement_later_registration_conflicts(self):
        pause_at = self.t0 + timedelta(days=30)
        paused = self.svc.request_supplement(
            self.report_cn["id"], "lead-cn", "regional_lead", "CN",
            {"authority": "NMPA", "requested_at": iso(pause_at)})
        sid = paused["supplement_request_id"]
        # 登记早的 B 先被采用
        win = self.svc.submit_supplement(
            sid, "reporter-b", "reporter", "CN",
            {"documents": ["b.pdf"], "registered_at": iso(pause_at + timedelta(days=1))})
        self.assertEqual(win["supplement"]["submitted_by"], "reporter-b")
        # 登记晚的 A 后到 -> 冲突
        with self.assertRaises(ApiError) as ctx:
            self.svc.submit_supplement(
                sid, "reporter-a", "reporter", "CN",
                {"documents": ["a.pdf"], "registered_at": iso(pause_at + timedelta(days=3))})
        self.assertEqual(ctx.exception.code, "supplement_conflict")
        # 胜者本人重复提交 -> 幂等
        again = self.svc.submit_supplement(
            sid, "reporter-b", "reporter", "CN",
            {"documents": ["b.pdf"], "registered_at": iso(pause_at + timedelta(days=1))})
        self.assertTrue(again["idempotent"])

    def test_earlier_registration_arriving_late_supersedes_winner(self):
        pause_at = self.t0 + timedelta(days=30)
        paused = self.svc.request_supplement(
            self.report_cn["id"], "lead-cn", "regional_lead", "CN",
            {"authority": "NMPA", "requested_at": iso(pause_at)})
        sid = paused["supplement_request_id"]
        first = self.svc.submit_supplement(
            sid, "reporter-a", "reporter", "CN",
            {"documents": ["a.pdf"], "registered_at": iso(pause_at + timedelta(days=3))})
        due_after_first = first["report"]["due_at"]
        # 登记时间更早的 B 在竞争中晚到达：收到冲突提示，但系统改用其资料；
        # 时钟只恢复一次，到期时间不变。
        with self.assertRaises(ApiError) as ctx:
            self.svc.submit_supplement(
                sid, "reporter-b", "reporter", "CN",
                {"documents": ["b.pdf"], "registered_at": iso(pause_at + timedelta(days=1))})
        self.assertEqual(ctx.exception.code, "supplement_conflict")
        req = self.svc.repo.conn.execute(
            "SELECT submitted_by FROM supplement_requests WHERE id=?", (sid,)).fetchone()
        self.assertEqual(req["submitted_by"], "reporter-b")
        report = self.svc.repo.conn.execute(
            "SELECT clock_status,due_at FROM reports WHERE id=?", (self.report_cn["id"],)).fetchone()
        self.assertEqual(report["clock_status"], "running")
        self.assertEqual(report["due_at"], due_after_first)

    def test_recalculation_failure_keeps_original_clock_and_retries(self):
        pause_at = self.t0 + timedelta(days=20)
        paused = self.svc.request_supplement(
            self.report_cn["id"], "lead-cn", "regional_lead", "CN",
            {"authority": "NMPA", "requested_at": iso(pause_at)})
        original_remaining = paused["remaining_seconds"]

        def broken(**kwargs):
            raise RuntimeError("upstream clock service down")

        self.svc.clock_recalculator = broken
        failed = self.svc.recalculate_clock(
            self.report_cn["id"], "lead-cn", "regional_lead", "CN")
        self.assertFalse(failed["recalculated"])
        # 原计时保留，且可看出重算失败
        row = next(r for r in self.svc.paused("regional_lead", "CN") if r["id"] == self.report_cn["id"])
        self.assertEqual(row["remaining_seconds"], original_remaining)
        self.assertTrue(row["recalc_failed"])

        # 恢复服务后重试成功
        from app import recalculate_remaining_seconds
        self.svc.clock_recalculator = recalculate_remaining_seconds
        retried = self.svc.recalculate_clock(
            self.report_cn["id"], "lead-cn", "regional_lead", "CN")
        self.assertTrue(retried["recalculated"])
        self.assertEqual(retried["remaining_seconds"], original_remaining)
        self.assertFalse(retried["report"]["recalc_failed"])

    def test_running_clock_recalc_rejected_and_permissions(self):
        with self.assertRaises(ApiError) as ctx:
            self.svc.recalculate_clock(self.report_cn["id"], "lead-cn", "regional_lead", "CN")
        self.assertEqual(ctx.exception.code, "clock_running")
        # reporter 不能登记补件要求
        with self.assertRaises(ApiError) as ctx:
            self.svc.request_supplement(
                self.report_cn["id"], "reporter-a", "reporter", "CN", {"authority": "NMPA"})
        self.assertEqual(ctx.exception.status, 403)
        # medical_reviewer 不能提交补件资料
        paused = self.svc.request_supplement(
            self.report_cn["id"], "lead-cn", "regional_lead", "CN", {"authority": "NMPA"})
        with self.assertRaises(ApiError) as ctx:
            self.svc.submit_supplement(
                paused["supplement_request_id"], "reviewer-1", "medical_reviewer", "",
                {"documents": ["x.pdf"]})
        self.assertEqual(ctx.exception.status, 403)

    def test_concurrent_submissions_single_winner(self):
        pause_at = self.t0 + timedelta(days=5)
        paused = self.svc.request_supplement(
            self.report_cn["id"], "lead-cn", "regional_lead", "CN",
            {"authority": "NMPA", "requested_at": iso(pause_at)})
        sid = paused["supplement_request_id"]
        results: list[Exception | dict] = []

        def worker(actor: str, registered_offset_seconds: int):
            try:
                results.append(self.svc.submit_supplement(
                    sid, actor, "reporter", "CN",
                    {"documents": [f"{actor}.pdf"],
                     "registered_at": iso(pause_at + timedelta(seconds=registered_offset_seconds))}))
            except ApiError as exc:
                results.append(exc)

        t1 = threading.Thread(target=worker, args=("reporter-a", 2))
        t2 = threading.Thread(target=worker, args=("reporter-b", 1))
        t1.start(); t2.start(); t1.join(); t2.join()
        conflicts = [r for r in results if isinstance(r, ApiError) and r.code == "supplement_conflict"]
        winners = [r for r in results if not isinstance(r, ApiError)]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(conflicts), 1)
        # 登记早 1 秒的 reporter-b 最终是被采用者
        req = self.svc.repo.conn.execute(
            "SELECT submitted_by,registered_at FROM supplement_requests WHERE id=?", (sid,)).fetchone()
        self.assertEqual(req["submitted_by"], "reporter-b")


if __name__ == "__main__":
    unittest.main()
