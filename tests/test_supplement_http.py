import json
import sys
import tempfile
import threading
import unittest
from datetime import timedelta
from http.client import HTTPConnection
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import create_server, iso, utcnow


def request(host, port, method, path, body=None, headers=None):
    conn = HTTPConnection(host, port, timeout=5)
    payload = None
    hdr = headers or {}
    if body is not None:
        payload = json.dumps(body).encode()
        hdr = {"Content-Type": "application/json", **hdr}
    conn.request(method, path, body=payload, headers=hdr)
    resp = conn.getresponse()
    raw = resp.read().decode()
    conn.close()
    data = json.loads(raw) if raw else {}
    return resp.status, data


class SupplementHttpTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.server = create_server(Path(self.tmp.name) / "http.db", "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.port}"
        self.lead = {"X-User-Id": "lead", "X-Role": "regional_lead", "X-Region": "CN"}
        self.reporter = {"X-User-Id": "ra", "X-Role": "reporter", "X-Region": "CN"}

    def tearDown(self):
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.server.server_close()
        self.tmp.cleanup()

    def req(self, method, path, body=None, headers=None):
        return request("127.0.0.1", self.port, method, path, body, headers)

    def test_pause_resume_and_overdue_visibility_over_http(self):
        t0 = utcnow().replace(microsecond=0)
        status, created = self.req("POST", "/api/cases", {
            "patient_ref": "P", "region": "CN", "product": "D", "event_term": "e",
            "source": "s", "dedupe_key": "http-1", "received_at": iso(t0)},
            {"X-User-Id": "ra", "X-Role": "reporter", "X-Region": "CN"})
        self.assertEqual(status, 201)
        cid = created["case"]["id"]
        _, cn = self.req("POST", f"/api/cases/{cid}/reports", {"country": "CN"}, self.lead)
        _, us = self.req("POST", f"/api/cases/{cid}/reports", {"country": "US"}, self.lead)

        pause_at = t0 + timedelta(days=10)
        status, paused = self.req("POST", f"/api/reports/{cn['id']}/request-supplement",
                                  {"authority": "NMPA", "requested_at": iso(pause_at)}, self.lead)
        self.assertEqual(status, 201)
        self.assertEqual(paused["report"]["clock_status"], "paused")
        self.assertAlmostEqual(paused["remaining_seconds"], 80 * 86400, delta=2)
        sid = paused["supplement_request_id"]

        # 暂停中的 CN 不进逾期清单；state.paused 能看出已暂停
        _, state = self.req("GET", "/api/state", headers={"X-User-Id": "ga", "X-Role": "global_admin"})
        paused_ids = {r["id"] for r in state["paused"]}
        self.assertIn(cn["id"], paused_ids)
        overdue_ids = {r["id"] for r in state["overdue"]}
        self.assertNotIn(cn["id"], overdue_ids)

        # 早登记的补件先到齐：到期 = 恢复时刻 + 80 天，时钟恢复
        resume_at = t0 + timedelta(days=150)
        status, resumed = self.req("POST", f"/api/supplements/{sid}/submissions", {
            "documents": ["early.pdf"],
            "registered_at": iso(pause_at + timedelta(days=1)),
            "resumed_at": iso(resume_at)}, self.reporter)
        self.assertEqual(status, 201)
        self.assertEqual(resumed["report"]["clock_status"], "running")
        self.assertEqual(resumed["new_due_at"], iso(resume_at + timedelta(days=80)))

        # 恢复后更晚登记的第二笔补件 -> 409 冲突
        status, err = self.req("POST", f"/api/supplements/{sid}/submissions",
                               {"documents": ["late.pdf"],
                                "registered_at": iso(pause_at + timedelta(days=9))},
                               {"X-User-Id": "rb", "X-Role": "reporter", "X-Region": "CN"})
        self.assertEqual(status, 409)
        self.assertEqual(err["error"], "supplement_conflict")


if __name__ == "__main__":
    unittest.main()
