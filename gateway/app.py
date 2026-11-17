"""HTTP 入口网关（FastAPI + httpx）。

职责：
  * 仅转发 GET 请求到上游，按轮询（round-robin）选择「健康且未排空」的节点；
  * 周期性主动探测上游，探测结论只影响健康标志，绝不改动人工排空标志；
  * 提供排空 / 恢复 / 状态管理 API；所有节点都不可接流时返回 503；
  * 排空期间新请求不再进入该节点，已在途的请求继续等待原响应返回；
  * 排空 API 阻塞到在途请求全部结束后才报告 drained=true；
  * 超时 / 连接错误 / 5xx 不虚报成功，并同步刷新健康状态，在途计数靠 try/finally 保证准确；
  * 按请求触发的影子核对（X-Compare-With）：主请求照常及时返回，另向健康且未排空的
    目标节点发送同路径同查询串的影子 GET，核对编号经响应头返回，结果可经管理 API 查询；
    影子请求计入目标节点在途，排空与核对并发时不会提前报告可停机。
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

# ----------------------------- 配置 -----------------------------

UPSTREAMS_RAW = os.environ.get(
    "UPSTREAMS",
    "node1=http://upstream1:8000,node2=http://upstream2:8000",
)
PROBE_INTERVAL = float(os.environ.get("PROBE_INTERVAL", "2.0"))
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "1.5"))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "15.0"))
DRAIN_DEFAULT_TIMEOUT = float(os.environ.get("DRAIN_DEFAULT_TIMEOUT", "30.0"))
# 影子请求超时；默认与主请求超时一致
COMPARE_TIMEOUT = float(os.environ.get("COMPARE_TIMEOUT", str(REQUEST_TIMEOUT)))
# 核对记录保留上限；待完成记录占满后新核对请求会被标明跳过
COMPARE_MAX_RECORDS = int(os.environ.get("COMPARE_MAX_RECORDS", "100"))
# 响应体摘要保留的预览长度（字节）
SUMMARY_PREVIEW_BYTES = int(os.environ.get("SUMMARY_PREVIEW_BYTES", "256"))

# 核对状态枚举
STATUS_PENDING = "pending"          # 待完成
STATUS_SAME = "same"                # 相同
STATUS_DIFFERENT = "different"      # 不同
STATUS_UNCOMPARABLE = "uncomparable"  # 无法核对
TERMINAL_STATUSES = {STATUS_SAME, STATUS_DIFFERENT, STATUS_UNCOMPARABLE}

# 透传响应时需要剥离的逐跳（hop-by-hop）头
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "content-encoding",
}


def _parse_upstreams(raw: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        name, _, url = item.partition("=")
        if not _:
            raise ValueError(f"非法 UPSTREAMS 条目: {item!r}，应为 name=url")
        out.append((name.strip(), url.strip().rstrip("/")))
    if not out:
        raise ValueError("至少需要配置一个上游节点")
    return out


def _summarize_response(resp: httpx.Response) -> dict:
    """提取状态码与响应体摘要（长度、类型、预览），不保留完整响应体。"""
    body = resp.content
    return {
        "status_code": resp.status_code,
        "content_length": len(body),
        "content_type": resp.headers.get("content-type"),
        "body_preview": body[:SUMMARY_PREVIEW_BYTES].decode(
            "utf-8", errors="replace"
        ),
    }


# ----------------------------- 核对记录 -----------------------------

@dataclass
class CompareRecord:
    id: str
    path: str
    query: str
    primary_node: Optional[str]
    target_node: Optional[str]
    status: str = STATUS_PENDING
    reason: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None
    primary: Optional[dict] = None
    shadow: Optional[dict] = None

    def snapshot(self) -> dict:
        return {
            "id": self.id,
            "status": self.status,
            "reason": self.reason,
            "path": self.path,
            "query": self.query,
            "primary_node": self.primary_node,
            "target_node": self.target_node,
            "created_at": self.created_at,
            "finished_at": self.finished_at,
            "primary": self.primary,
            "shadow": self.shadow,
        }


class CompareStore:
    """核对记录存储：最多保留 max_records 条，待完成记录占满时跳过新核对。"""

    def __init__(self, max_records: int = COMPARE_MAX_RECORDS) -> None:
        self.max_records = max_records
        self._records: list[CompareRecord] = []
        self._lock = asyncio.Lock()
        # 因存储占满（全部是待完成）而跳过的核对计数
        self.skipped = 0

    async def create(
        self,
        *,
        path: str,
        query: str,
        primary_node: Optional[str],
        target_node: Optional[str],
    ) -> Optional[CompareRecord]:
        """登记一条待完成记录；容量不足时先淘汰最旧的已完成记录，仍不足则跳过。"""
        async with self._lock:
            if len(self._records) >= self.max_records:
                for idx, rec in enumerate(self._records):
                    if rec.status in TERMINAL_STATUSES:
                        del self._records[idx]
                        break
                else:
                    # 全部是待完成记录：不能淘汰，跳过本次核对
                    self.skipped += 1
                    return None
            rec = CompareRecord(
                id=uuid.uuid4().hex,
                path=path,
                query=query,
                primary_node=primary_node,
                target_node=target_node,
            )
            self._records.append(rec)
            return rec

    async def finish(self, rec: CompareRecord, status: str, **fields) -> None:
        async with self._lock:
            rec.status = status
            rec.finished_at = time.time()
            for key, value in fields.items():
                setattr(rec, key, value)

    async def get(self, rec_id: str) -> Optional[CompareRecord]:
        async with self._lock:
            for rec in self._records:
                if rec.id == rec_id:
                    return rec
            return None

    async def list_snapshot(self) -> dict:
        async with self._lock:
            counts = {
                STATUS_PENDING: 0,
                STATUS_SAME: 0,
                STATUS_DIFFERENT: 0,
                STATUS_UNCOMPARABLE: 0,
            }
            for rec in self._records:
                counts[rec.status] += 1
            return {
                "max_records": self.max_records,
                "stored": len(self._records),
                "skipped_full": self.skipped,
                "counts": counts,
                # 最新的在前
                "records": [rec.snapshot() for rec in reversed(self._records)],
            }


# ----------------------------- 节点状态 -----------------------------

@dataclass
class NodeState:
    name: str
    base_url: str
    # 健康标志：只由探测任务（以及请求失败时的降级）写入
    healthy: bool = False
    # 排空标志：只由人工 drain/resume 写入，探测永不触碰
    draining: bool = False
    inflight: int = 0
    rr_hits: int = 0
    success_count: int = 0
    failure_count: int = 0
    # 累计承接的影子核对请求数（在途影子仍计入 inflight）
    shadow_count: int = 0
    last_probe_at: Optional[float] = None
    last_probe_ok: Optional[bool] = None
    last_probe_detail: str = "尚未探测"
    last_change_at: float = field(default_factory=time.time)

    def can_serve(self) -> bool:
        return self.healthy and not self.draining

    def snapshot(self) -> dict:
        return {
            "name": self.name,
            "base_url": self.base_url,
            "healthy": self.healthy,
            "draining": self.draining,
            "eligible": self.can_serve(),
            "inflight": self.inflight,
            "rr_hits": self.rr_hits,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "shadow_count": self.shadow_count,
            "last_probe_at": self.last_probe_at,
            "last_probe_ok": self.last_probe_ok,
            "last_probe_detail": self.last_probe_detail,
        }


class Gateway:
    def __init__(self, nodes: list[NodeState]) -> None:
        self.nodes = nodes
        self._by_name = {n.name: n for n in nodes}
        # 一把条件变量保护所有节点状态；请求结束 / 排空等待都在其上通知
        self.cond = asyncio.Condition()
        self._rr = 0
        self.client: Optional[httpx.AsyncClient] = None
        self._probe_task: Optional[asyncio.Task] = None
        self.compares = CompareStore()
        # 后台影子任务的强引用，避免被 GC；任务结束后自行移除
        self._bg_tasks: set[asyncio.Task] = set()

    def get(self, name: str) -> Optional[NodeState]:
        return self._by_name.get(name)

    async def pick_node(self) -> Optional[NodeState]:
        """轮询选取健康且未排空的节点，并立即登记在途计数。"""
        async with self.cond:
            n = len(self.nodes)
            for offset in range(n):
                idx = (self._rr + offset) % n
                node = self.nodes[idx]
                if node.can_serve():
                    self._rr = (idx + 1) % n
                    node.inflight += 1
                    node.rr_hits += 1
                    return node
            return None

    async def release_node(self, node: NodeState, ok: bool) -> None:
        async with self.cond:
            node.inflight -= 1
            if ok:
                node.success_count += 1
            else:
                node.failure_count += 1
            # 在途归零可能让 drain 等待者满足条件
            self.cond.notify_all()

    async def mark_unhealthy(self, node: NodeState, detail: str) -> None:
        """请求失败时的即时降级；只改健康标志，不动 draining。"""
        async with self.cond:
            node.healthy = False
            node.last_probe_ok = False
            node.last_probe_detail = f"请求失败降级: {detail}"
            node.last_probe_at = time.time()
            node.last_change_at = time.time()

    async def probe_one(self, node: NodeState) -> None:
        """对单个节点做主动探测。成功只置 healthy=True，绝不解除 draining。"""
        assert self.client is not None
        try:
            resp = await self.client.get(
                f"{node.base_url}/", timeout=PROBE_TIMEOUT
            )
            ok = resp.status_code < 500
            detail = f"HTTP {resp.status_code}"
        except httpx.TimeoutException as exc:
            ok, detail = False, f"探测超时: {type(exc).__name__}"
        except httpx.RequestError as exc:
            ok, detail = False, f"探测连接错误: {type(exc).__name__}: {str(exc)[:100]}"

        async with self.cond:
            node.healthy = ok
            # 注意：这里刻意不修改 node.draining
            node.last_probe_at = time.time()
            node.last_probe_ok = ok
            node.last_probe_detail = detail
            node.last_change_at = time.time()

    async def probe_all(self) -> None:
        await asyncio.gather(*(self.probe_one(n) for n in self.nodes))

    async def _probe_loop(self) -> None:
        while True:
            try:
                await self.probe_all()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # 探测循环本身不能崩
                print(f"[probe-loop] unexpected error: {exc!r}")
            await asyncio.sleep(PROBE_INTERVAL)

    async def drain(
        self, node: NodeState, timeout: Optional[float]
    ) -> tuple[bool, int]:
        """置排空标志并等待在途清零。返回 (是否排空完成, 剩余在途数)。"""
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + max(timeout, 0.0)
        async with self.cond:
            node.draining = True
            node.last_change_at = time.time()
            while node.inflight > 0:
                if deadline is None:
                    await self.cond.wait()
                    continue
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return False, node.inflight
                # asyncio.Condition.wait 不支持 timeout，用 wait_for 限时等待
                try:
                    await asyncio.wait_for(self.cond.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    pass  # 回到循环重新计算剩余时间 / 在途数
            return True, 0

    async def resume(self, node: NodeState) -> None:
        """恢复接流：先清排空并保守置为不健康，再立即探测，由探测决定能否接流。"""
        async with self.cond:
            node.draining = False
            # 恢复不绕过故障探测：探测结论出来前一律不接流，杜绝状态窗口
            node.healthy = False
            node.last_probe_ok = None
            node.last_probe_detail = "恢复接流，等待首次探测确认"
            node.last_change_at = time.time()
        await self.probe_one(node)

    # ----------------------------- 影子核对 -----------------------------

    async def prepare_comparison(
        self,
        *,
        target_name: str,
        path: str,
        query: str,
        query_items: list[tuple[str, str]],
        primary_node: Optional[str],
    ) -> "ComparePlan":
        """在主请求发出「之前」完成核对预检并立即派发影子任务。

        关键：影子 HTTP 请求必须与主请求并发，因此本方法在主请求 await 之前调用。
        影子任务先等待主请求结果（future），拿到后再与自身响应比对，因此无需等待
        主请求结束即可开始占用目标节点在途。

        返回 ComparePlan：
          * rec 为 None：待完成记录占满，本次核对跳过（skip_reason 标明，主响应不受影响）；
          * task 为 None：预检直接落为 uncomparable（目标未知 / 同节点 / 排空 / 不健康）；
          * 否则 task 为已在途运行的影子任务。
        """
        target = self.get(target_name)
        if target is None:
            reason = "unknown_target"
        elif target.name == primary_node:
            reason = "same_node_as_primary"
        elif target.draining:
            reason = "target_draining"
        elif not target.healthy:
            reason = "target_unhealthy"
        else:
            reason = None

        rec = await self.compares.create(
            path=path, query=query,
            primary_node=primary_node, target_node=target_name,
        )
        if rec is None:
            return ComparePlan(rec=None, skip_reason="store_full_pending")

        if reason is not None:
            await self.compares.finish(
                rec, STATUS_UNCOMPARABLE, reason=reason
            )
            return ComparePlan(rec=rec)

        # 主请求结果占位：proxy 拿到主响应后通过 settle 回填
        primary_future: asyncio.Future = asyncio.get_running_loop().create_future()
        task = asyncio.create_task(
            self._run_shadow(rec, target, path, query_items, primary_future)
        )
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)
        return ComparePlan(rec=rec, task=task, primary_future=primary_future)

    async def settle_comparison(
        self,
        plan: "ComparePlan",
        *,
        primary_failure: Optional[str],
        primary_summary: Optional[dict],
        primary_body: Optional[bytes],
    ) -> None:
        """主请求结束后回填结果。

        主请求传输层失败（无主响应）时直接把记录置为 uncomparable 并取消尚未发出
        影子请求的任务；成功则把主响应交给正在等待的影子任务继续比对。
        预检已终态化（如目标不可用）的计划，本方法幂等跳过。
        """
        if plan.rec is None:
            return  # 本次核对因存储占满被跳过
        if plan.task is None or plan.primary_future is None:
            return  # 预检已直接落为 uncomparable，无需收尾
        if primary_failure is not None:
            # 主请求失败：绝不能报相同
            plan.primary_future.set_exception(
                ComparePrimaryFailed(primary_failure)
            )
            return
        plan.primary_future.set_result((primary_summary, primary_body))

    async def _run_shadow(
        self,
        rec: CompareRecord,
        target: NodeState,
        path: str,
        query_items: list[tuple[str, str]],
        primary_future: "asyncio.Future",
    ) -> None:
        """影子请求：与主请求同路径同查询串，计入目标节点在途，不影响主响应。

        时序要点：先预留目标在途并立即发出影子 HTTP（与主请求并发），再等待主
        请求结果。这样影子在途与主请求在途同时存在，排空必须等待两者都结束。
        """
        target_url = f"{target.base_url}/{path}"
        reserved = False
        shadow_task: Optional[asyncio.Task] = None
        try:
            # 1) 登记在途前再次确认可接流：与排空并发时，排空后不得再启动影子请求
            async with self.cond:
                if not target.can_serve():
                    await self._finish_waiting_primary(
                        rec, primary_future, "target_became_unavailable"
                    )
                    return
                target.inflight += 1
                target.shadow_count += 1
                reserved = True

            assert self.client is not None
            # 2) 立即发出影子请求，不等待主请求结果，保证与主请求并发
            shadow_task = asyncio.create_task(
                self.client.request(
                    "GET",
                    target_url,
                    params=query_items,
                    timeout=COMPARE_TIMEOUT,
                    headers={"X-Shadow-Compare": rec.id},
                )
            )

            # 3) 等待主请求结果（此时影子 HTTP 已在途）
            try:
                primary_summary, primary_body = await primary_future
            except ComparePrimaryFailed as exc:
                # 主请求失败：绝不能报相同；取消仍在途的影子请求并归还在途
                shadow_task.cancel()
                await self.compares.finish(
                    rec, STATUS_UNCOMPARABLE,
                    reason=f"primary_failed:{exc.reason}",
                )
                return

            # 4) 等待影子响应并与主响应比对
            try:
                shadow_resp = await shadow_task
            except asyncio.CancelledError:
                raise
            except httpx.TimeoutException as exc:
                # 目标超时：不能报相同
                await self.compares.finish(
                    rec, STATUS_UNCOMPARABLE,
                    reason=f"shadow_timeout:{type(exc).__name__}",
                    primary=primary_summary,
                )
                return
            except httpx.RequestError as exc:
                # 目标不可达：不能报相同
                await self.compares.finish(
                    rec, STATUS_UNCOMPARABLE,
                    reason=f"shadow_unreachable:{type(exc).__name__}",
                    primary=primary_summary,
                )
                return

            shadow_summary = _summarize_response(shadow_resp)
            # 相同 = 状态码一致且响应体字节一致；其余即不同
            is_same = (
                shadow_resp.status_code == primary_summary["status_code"]
                and shadow_resp.content == primary_body
            )
            await self.compares.finish(
                rec,
                STATUS_SAME if is_same else STATUS_DIFFERENT,
                primary=primary_summary,
                shadow=shadow_summary,
            )
        except asyncio.CancelledError:
            if shadow_task is not None:
                shadow_task.cancel()
            raise
        except Exception as exc:  # 影子任务自身异常也必须给记录一个终态
            await self.compares.finish(
                rec, STATUS_UNCOMPARABLE,
                reason=f"shadow_internal_error:{type(exc).__name__}:{str(exc)[:80]}",
            )
        finally:
            if reserved:
                async with self.cond:
                    target.inflight -= 1
                    # 影子在途归零可能让 drain 等待者满足条件
                    self.cond.notify_all()

    async def _finish_waiting_primary(
        self,
        rec: CompareRecord,
        primary_future: "asyncio.Future",
        reason: str,
    ) -> None:
        """目标在预检后不可用时收尾：等主结果回填（若成功）后置 uncomparable。"""
        try:
            primary_summary, _ = await primary_future
        except ComparePrimaryFailed as exc:
            await self.compares.finish(
                rec, STATUS_UNCOMPARABLE,
                reason=f"primary_failed:{exc.reason}",
            )
            return
        await self.compares.finish(
            rec, STATUS_UNCOMPARABLE, reason=reason, primary=primary_summary
        )


class ComparePrimaryFailed(Exception):
    """主请求传输层失败信号，用于唤醒等待主结果的影子任务。"""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass
class ComparePlan:
    """一次核对的预检结果与并发句柄。"""

    rec: Optional[CompareRecord]
    task: Optional[asyncio.Task] = None
    primary_future: Optional["asyncio.Future"] = None
    skip_reason: Optional[str] = None


# ----------------------------- 应用生命周期 -----------------------------

gateway = Gateway([NodeState(name, url) for name, url in _parse_upstreams(UPSTREAMS_RAW)])


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with httpx.AsyncClient() as client:
        gateway.client = client
        # 启动先做一轮探测，避免把流量打到未知状态的节点
        await gateway.probe_all()
        gateway._probe_task = asyncio.create_task(gateway._probe_loop())
        try:
            yield
        finally:
            if gateway._probe_task:
                gateway._probe_task.cancel()
                try:
                    await gateway._probe_task
                except asyncio.CancelledError:
                    pass
            # 等待仍在途的影子请求收尾，避免进程退出时记录悬空
            if gateway._bg_tasks:
                await asyncio.gather(*gateway._bg_tasks, return_exceptions=True)


app = FastAPI(
    title="http-entry-gateway",
    version="1.1.0",
    description=(
        "仅转发 GET 的入口网关：轮询健康且未排空的上游，支持排空/恢复/状态管理，"
        "无可接流节点时返回 503；支持按请求触发的影子核对（X-Compare-With）。"
    ),
    lifespan=lifespan,
)


# ----------------------------- 转发接口 -----------------------------

@app.get("/", tags=["meta"])
async def root() -> dict:
    return {
        "service": "http-entry-gateway",
        "proxy": "GET /proxy/{path} 仅支持 GET，可带 X-Compare-With 头触发影子核对",
        "admin": [
            "GET  /admin/status",
            "POST /admin/nodes/{name}/drain?timeout=",
            "POST /admin/nodes/{name}/resume",
            "POST /admin/probe",
            "GET  /admin/compares",
            "GET  /admin/compares/{compare_id}",
        ],
    }


@app.get("/proxy/{path:path}", tags=["proxy"])
async def proxy(path: str, request: Request) -> Response:
    compare_target = request.headers.get("x-compare-with")
    if compare_target is not None:
        compare_target = compare_target.strip() or None

    node = await gateway.pick_node()
    if node is None:
        response: Response = JSONResponse(
            status_code=503,
            content={
                "error": "no_eligible_upstream",
                "detail": "当前没有健康且未排空的上游节点可接流",
            },
        )
        # 即便主请求失败，也要给核对一个终态（无法核对），绝不能报相同
        if compare_target is not None:
            plan503 = await gateway.prepare_comparison(
                target_name=compare_target,
                path=path,
                query=request.url.query,
                query_items=list(request.query_params.multi_items()),
                primary_node=None,
            )
            # 无主节点：预检若未直接终态化，也要按主失败收尾
            await gateway.settle_comparison(
                plan503,
                primary_failure="no_eligible_upstream",
                primary_summary=None,
                primary_body=None,
            )
            _apply_plan_headers(response, plan503)
        return response

    target_url = f"{node.base_url}/{path}"
    ok = False
    primary_failure: Optional[str] = None
    primary_summary: Optional[dict] = None
    primary_body: Optional[bytes] = None

    # 影子核对预检：在主请求发出之前派发影子任务，使其与主请求真正并发
    plan = None
    if compare_target is not None:
        plan = await gateway.prepare_comparison(
            target_name=compare_target,
            path=path,
            query=request.url.query,
            query_items=list(request.query_params.multi_items()),
            primary_node=node.name,
        )

    try:
        try:
            upstream_resp = await gateway.client.request(
                "GET",
                target_url,
                params=request.query_params,
                timeout=REQUEST_TIMEOUT,
            )
        except httpx.TimeoutException as exc:
            primary_failure = f"timeout:{type(exc).__name__}"
            await gateway.mark_unhealthy(node, f"请求超时 {type(exc).__name__}")
            response = JSONResponse(
                status_code=504,
                content={"error": "upstream_timeout", "node": node.name},
            )
        except httpx.RequestError as exc:
            primary_failure = f"unreachable:{type(exc).__name__}"
            await gateway.mark_unhealthy(
                node, f"{type(exc).__name__}: {str(exc)[:100]}"
            )
            response = JSONResponse(
                status_code=502,
                content={"error": "upstream_unreachable", "node": node.name},
            )

        if primary_failure is None:
            # 5xx 视为上游故障：如实透传状态码并降级，绝不虚报成功
            if upstream_resp.status_code >= 500:
                await gateway.mark_unhealthy(
                    node, f"上游返回 HTTP {upstream_resp.status_code}"
                )

            ok = upstream_resp.status_code < 500
            headers = {
                k: v
                for k, v in upstream_resp.headers.items()
                if k.lower() not in _HOP_BY_HOP
            }
            headers["X-Gateway"] = "http-entry-gateway"
            headers["X-Upstream-Node"] = node.name
            response = Response(
                content=upstream_resp.content,
                status_code=upstream_resp.status_code,
                headers=headers,
                media_type=upstream_resp.headers.get("content-type"),
            )
            primary_summary = _summarize_response(upstream_resp)
            primary_body = upstream_resp.content
    finally:
        # 无论成功、失败还是取消，在途计数都必须准确归还
        await gateway.release_node(node, ok)

    if plan is not None:
        # 回填主请求结果，唤醒等待中的影子任务；主失败则置 uncomparable
        await gateway.settle_comparison(
            plan,
            primary_failure=primary_failure,
            primary_summary=primary_summary,
            primary_body=primary_body,
        )
        _apply_plan_headers(response, plan)
    return response


def _apply_plan_headers(response: Response, plan: "ComparePlan") -> None:
    """把核对编号 / 跳过标记写入主响应头。"""
    if plan.rec is not None:
        response.headers["X-Compare-Id"] = plan.rec.id
    if plan.skip_reason is not None:
        # 待完成记录占满：标明跳过，主响应照常返回
        response.headers["X-Compare-Skipped"] = "1"
        response.headers["X-Compare-Skip-Reason"] = plan.skip_reason


# 非 GET 方法访问转发路径：FastAPI 自动返回 405 Method Not Allowed


# ----------------------------- 管理接口 -----------------------------

@app.get("/admin/status", tags=["admin"])
async def admin_status() -> dict:
    async with gateway.cond:
        nodes = [n.snapshot() for n in gateway.nodes]
        eligible = [n.name for n in gateway.nodes if n.can_serve()]
    compare_stats = await gateway.compares.list_snapshot()
    return {
        "probe_interval": PROBE_INTERVAL,
        "request_timeout": REQUEST_TIMEOUT,
        "compare_timeout": COMPARE_TIMEOUT,
        "nodes": nodes,
        "eligible_nodes": eligible,
        "comparisons": {
            "max_records": compare_stats["max_records"],
            "stored": compare_stats["stored"],
            "skipped_full": compare_stats["skipped_full"],
            "counts": compare_stats["counts"],
        },
    }


@app.post("/admin/nodes/{name}/drain", tags=["admin"])
async def admin_drain(name: str, timeout: Optional[float] = DRAIN_DEFAULT_TIMEOUT):
    """排空节点：立即停止分配新请求（含影子请求），并阻塞等待在途请求全部结束。

    只有在途数归零才返回 drained=true；超时仍有在途请求时返回 202，
    drained=false 且节点继续保持排空状态。
    """
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    drained, inflight = await gateway.drain(node, timeout)
    body = {
        "node": name,
        "draining": True,
        "drained": drained,
        "inflight": inflight,
        "detail": "在途请求已全部结束" if drained else "等待在途请求结束超时，节点仍处于排空中",
    }
    return JSONResponse(status_code=200 if drained else 202, content=body)


@app.post("/admin/nodes/{name}/resume", tags=["admin"])
async def admin_resume(name: str):
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    await gateway.resume(node)
    snap = node.snapshot()
    return {
        "node": name,
        "draining": False,
        "healthy": snap["healthy"],
        "eligible": snap["eligible"],
        "last_probe_detail": snap["last_probe_detail"],
        "detail": "已解除排空；是否可接流以健康探测结论为准",
    }


@app.post("/admin/probe", tags=["admin"])
async def admin_probe() -> dict:
    """手动触发一轮探测（探测不会改变任何节点的排空标志）。"""
    await gateway.probe_all()
    async with gateway.cond:
        return {"nodes": [n.snapshot() for n in gateway.nodes]}


@app.get("/admin/compares", tags=["compare"])
async def admin_list_compares() -> dict:
    """列出核对记录（最新在前）及各状态计数、跳过计数。"""
    return await gateway.compares.list_snapshot()


@app.get("/admin/compares/{compare_id}", tags=["compare"])
async def admin_get_compare(compare_id: str):
    rec = await gateway.compares.get(compare_id)
    if rec is None:
        return JSONResponse(
            status_code=404,
            content={"error": "compare_not_found", "id": compare_id},
        )
    return rec.snapshot()
