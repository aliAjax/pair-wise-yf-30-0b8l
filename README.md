# 药物警戒案例处理系统

使用 Python 标准库实现的独立原型，覆盖多渠道案例接入、去重、随访更正、严重性医学裁定、分国家报告、逾期升级、跨区域权限和案例合并审计。

## 运行

要求 Python 3.11+。

```bash
python3 app.py --db pharmacovigilance.db
```

默认监听 `127.0.0.1:8201`。首页为 `http://127.0.0.1:8201/`，健康检查为 `/health`。

所有接口使用请求头 `X-User-Id`、`X-Role` 和区域角色必需的 `X-Region`。角色为 `reporter`、`regional_lead`、`medical_reviewer`、`global_admin`。

## 主要接口

- `POST /api/cases`：录入案例，`dedupe_key` 相同则返回已存在案例。
- `GET /api/cases`、`GET /api/cases/{id}`：按权限查询。
- `POST /api/cases/{id}/followups`：用 `expected_revision` 防止覆盖随访。
- `POST /api/cases/{id}/medical-review`：医学审核员更新严重性、死亡和关联性。
- `POST /api/cases/{id}/reports`、`POST /api/reports/{id}/submit`：生成并提交分国家报告。
- `POST /api/reports/{id}/request-supplement`：监管发来补件要求，**暂停该国家报告**的时限时钟（冻结 `due_at`、记录剩余秒数），其他国家不受影响；重复登记返回 `409 supplement_active`。
- `POST /api/supplements/{id}/submissions`：登记补件资料。最早登记的一份被采用并**按暂停时的剩余天数恢复该国时钟**（新到期 = 恢复时刻 + 剩余秒数）；两人同时提交时只认登记最早者，后到者收到 `409 supplement_conflict`。可传 `registered_at`（资料登记时间）与 `resumed_at`（恢复计时时刻）。
- `POST /api/reports/{id}/recalculate-clock`：对暂停中的报告按当前严重性重算剩余天数；重算失败时保留原计时并置 `recalc_failed=1`，可再次重试。
- `GET /api/paused`：查看所有暂停中的分国家报告。`GET /api/overdue` 只含时钟在计时且已逾期的报告（暂停中的不计逾期）；报告对象带 `clock_status`（`running`/`paused`）、`paused`、`remaining_seconds`、`recalc_failed` 字段。
- 暂停期间医学审核改变严重性（`/medical-review`）会自动重算该案例所有暂停中国家的剩余天数；运行中的国家和其他案例不受影响。
- `POST /api/cases/{id}/merge`：全局管理员合并重复案例。
- `POST /api/escalate-overdue`、`GET /api/overdue`：逾期检查与升级。

## 补件时限暂停/续算规则

- 收到补件要求时暂停该国时钟，剩余预算 = 原到期时间 − 暂停时刻。
- 资料到齐时按同一笔剩余预算续算：新到期 = 恢复时刻 + 剩余秒数，暂停期间不计入时限。
- 暂停期间严重性变化：剩余预算 = 新窗口到期 − 暂停时刻（可为负，恢复即逾期）。
- 补件并发：每个提交先独立登记（`supplement_submissions`，带唯一约束防同人重复），再对该补件要求加行锁串行裁决；只有登记最早者能原子认领（`awaiting → submitted`）并恢复时钟一次，其余收到冲突。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 主要局限

该实现使用请求头模拟身份，不含生产级登录、签名和密钥管理；SQLite 与标准库 HTTP 服务适合单机原型。分国家规则采用内置严重 15 天、死亡 7 天、非严重 90 天规则，接入真实监管网关前需按当地法规扩展。
