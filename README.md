# HTTP 入口网关（支持优雅排空 / 健康探测 / 轮询转发 / 影子核对）

滚动更新场景下的入口网关：运维人员先排空待停机实例，使新请求不再进入，
同时保证在途长请求完整取回原响应，全部结束后才报告排空完成。

切流前可用**按请求触发的影子核对**：客户端通过 `X-Compare-With` 指定另一节点，
主请求仍按原轮询规则及时返回，网关另向健康且未排空的目标发送同路径、同查询串的
影子 GET，并把核对编号经响应头返回，核对结论可经管理 API 查询。

技术栈：**FastAPI + httpx**，由 **Docker Compose** 启动 1 个网关 + 2 个可模拟
慢请求 / 故障的上游。

## 目录结构

```
.
├── docker-compose.yml      # 编排：gateway + upstream1 + upstream2
├── gateway/
│   ├── app.py              # 网关：仅转发 GET、轮询、探测、排空/恢复/状态、影子核对
│   ├── requirements.txt
│   └── Dockerfile
├── upstream/
│   ├── app.py              # 模拟上游：slow / flaky / same / 故障模式控制
│   ├── requirements.txt
│   └── Dockerfile
└── tests/
    ├── self_test.py        # 本地端到端自测（22 组场景，107 项断言）
    └── test_compare_store.py  # 核对记录存储纯逻辑自测（20 项断言）
```

## 一键启动

```bash
docker compose up --build
# 网关:        http://localhost:18080
# 上游 node1:  http://localhost:18081
# 上游 node2:  http://localhost:18082
# 交互文档:    http://localhost:18080/docs
```

网关环境变量（见 `docker-compose.yml`）：

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `UPSTREAMS` | `node1=http://upstream1:8000,node2=http://upstream2:8000` | 上游列表 `name=url` |
| `PROBE_INTERVAL` | `2.0` | 周期探测间隔（秒） |
| `PROBE_TIMEOUT` | `1.5` | 探测超时（秒） |
| `REQUEST_TIMEOUT` | `15.0` | 转发请求超时（秒） |
| `DRAIN_DEFAULT_TIMEOUT` | `30.0` | 排空 API 默认等待上限（秒） |
| `COMPARE_TIMEOUT` | 同 `REQUEST_TIMEOUT` | 影子请求超时（秒） |
| `COMPARE_MAX_RECORDS` | `100` | 核对记录保留上限；待完成占满后新核对标明跳过 |
| `SUMMARY_PREVIEW_BYTES` | `256` | 响应体摘要预览长度（字节） |

## API 一览（均可在 /docs 直接操作）

转发（仅 GET）：

```
GET /proxy/{path}      # 轮询转发到健康且未排空的节点，透传查询串与响应
                       # 无可用节点 -> 503；上游超时 -> 504；连不上 -> 502
                       # 非 GET 方法 -> 405
                       # 带 X-Compare-With: <节点名> 头即触发影子核对
```

影子核对：

```
请求头  X-Compare-With: <目标节点名>   # 主请求照常返回，另向目标发同路径同查询串影子 GET
响应头  X-Compare-Id: <核对编号>        # 凭编号查询结论
        X-Compare-Skipped: 1            # 仅在待完成记录占满、本次核对被跳过时出现
        X-Compare-Skip-Reason: store_full_pending

GET  /admin/compares                   # 列出记录（最新在前）+ 各状态计数 + 跳过计数
GET  /admin/compares/{compare_id}      # 查单条；未知编号 -> 404
```

核对状态：`pending`（待完成）、`same`（相同：状态码与响应体字节都一致）、
`different`（不同）、`uncomparable`（无法核对，附 `reason`）。
以下情况一律 `uncomparable` 且**绝不能报 same**：主请求失败（504/502/503）、
目标未知 / 与主节点相同 / 排空中 / 不健康、影子超时 / 不可达。
记录含双方状态码与响应体摘要（长度、类型、前 256 字节预览），不保留完整响应体。

管理：

```
GET  /admin/status                     # 全部节点健康/排空/在途/计数/最近探测 + 核对统计
POST /admin/nodes/{name}/drain?timeout=# 排空：立即停接新流（含影子），阻塞到在途清零
                                       #   在途归零 -> 200 drained=true
                                       #   超时仍有在途 -> 202 drained=false
POST /admin/nodes/{name}/resume        # 恢复：清排空，立即探测，由探测决定能否接流
POST /admin/probe                      # 手动触发一轮探测
```

上游模拟控制（演练用，直连上游端口，不经过网关）：

```
GET  /control/state            # 查看模式与在途数
POST /control/mode?mode=       # healthy | slow | error | hang
GET  /slow?seconds=N           # 慢请求（验证在途不被截断）
GET  /flaky                    # 按模式返回 200 / 500 / 挂起
GET  /same?shadow_delay=N      # 节点无关的稳定响应（验证 same）；影子请求可延迟 N 秒
GET  /                         # 健康端点（error->503, hang->挂起）
```

## 典型演练

```bash
# 1. 发一个慢请求（10 秒），在另一终端立刻排空 node2
curl "http://localhost:18080/proxy/slow?seconds=10" &
curl -X POST "http://localhost:18080/admin/nodes/node2/drain?timeout=30"
# 现象：drain 请求阻塞，约 10 秒后才返回 drained=true；慢请求拿到完整 200

# 2. 让 node1 进入故障模式，观察探测自动摘除
curl -X POST "http://localhost:18081/control/mode?mode=error"
sleep 3
curl http://localhost:18080/admin/status      # node1 healthy=false

# 3. 故障未消除时恢复 node1：不会绕过探测，节点仍不可接流
curl -X POST http://localhost:18080/admin/nodes/node1/resume
# eligible=false；上游修好后经周期探测自动恢复接流
```

## 关键设计与并发正确性

- **两个独立标志位**：`healthy` 只由探测（含请求失败降级）写入；`draining` 只由
  人工 drain/resume 写入。因此**探测成功永不解除排空**，两者交错互不串改。
- **恢复不绕过故障探测**：`resume` 先清排空并保守置 `healthy=false`，再做一次
  真实探测；探测通过前该节点不会被轮询选中。
- **轮询 + 原子登记**：在同一把 `asyncio.Condition` 锁内完成选节点与
  `inflight += 1`，保证"选中即可接流、计数即生效"。
- **在途计数可靠**：转发用 `try/finally` 归还计数，成功、5xx、超时、连接错误、
  客户端取消都会准确递减；排空等待在条件变量上，请求一结束即被唤醒。
- **排空语义**：置位后新请求立即绕过该节点；已在途请求不被取消，继续等待并
  透传原响应；只有 `inflight` 归零才返回 `drained=true`。
- **不虚报成功**：5xx 如实透传并即时降级；超时返回 504、连接错误返回 502，
  均标记失败并刷新健康状态，绝不返回虚假 200。
- **无节点可接流**：所有节点都不健康或排空中时，`GET /proxy/*` 返回 503。

### 影子核对的并发正确性

- **主请求不受拖累**：影子任务在主请求发出**之前**就派发，并立即发起影子 HTTP，
  两者真正并发；主请求按原轮询规则及时返回，不等影子结论。
- **影子计入目标在途**：影子请求与主请求共用节点 `inflight` 计数，登记在同一把
  条件变量锁内。因此**排空与核对并发时，排空必须等影子请求也结束**，
  不会提前报告 `drained=true`（可停机）。
- **排空后不再启动影子**：预检时校验目标 `healthy && !draining`，影子任务登记
  在途前在锁内二次确认 `can_serve()`；一旦排空，新的影子请求不会再发往该节点，
  记录落为 `uncomparable/target_draining` 或 `target_became_unavailable`。
- **绝不能报相同的边界**：主请求传输失败（504/502/503）、影子超时、影子不可达
  一律 `uncomparable`；仅当主、影子状态码与响应体字节都一致才判 `same`。
- **记录有界**：最多保留 `COMPARE_MAX_RECORDS`（默认 100）条；容量不足时优先淘汰
  最旧的已完成记录；若全部是待完成记录则跳过本次核对，响应头标明
  `X-Compare-Skipped: 1`，主响应照常返回。
- **关闭不悬空**：进程关闭时等待仍在途的影子任务收尾，记录不会停留无主的 pending。

## 典型影子核对演练

```bash
# 主请求照常返回，响应头带核对编号；后台向 node2 发同路径同查询串的影子 GET
curl -i -H 'X-Compare-With: node2' \
  "http://localhost:18080/proxy/same?a=1&b=2"
# 响应头: X-Compare-Id: <id>

# 凭编号查结论（same/different/uncomparable，附双方状态码与响应体摘要）
curl http://localhost:18080/admin/compares/<id>
curl http://localhost:18080/admin/compares        # 列表 + 各状态计数

# 核对与排空交错：发一个慢影子请求后立刻排空目标节点，
# drain 会阻塞到影子在途结束才返回 drained=true
curl -H 'X-Compare-With: node2' \
  "http://localhost:18080/proxy/slow?seconds=5" &
curl -X POST "http://localhost:18080/admin/nodes/node2/drain?timeout=30"
```

## 自测

```bash
# 无需 docker；需本地具备 fastapi/uvicorn/httpx（可 pip install 到独立目录）
python3 tests/self_test.py
python3 tests/test_compare_store.py
```

端到端自测覆盖 22 组场景、107 项断言，除原有的探测/排空/转发场景外，新增：
影子 same/different 判定与双方摘要、未知/同节点/排空/不健康目标的 uncomparable、
**影子在途阻塞排空（核对×排空交错）**、排空后不承接新影子、主请求失败不得报相同、
影子超时/不可达不得报相同、待完成记录占满跳过且主响应照常。
存储层另有 20 项断言覆盖 100 条上限、已完成淘汰、待完成占满跳过与计数自洽。
