#!/usr/bin/env python3
"""网关本地自测（不依赖 docker，使用 stdlib + 三个 uvicorn 子进程）。

覆盖关键场景：
  1.  轮询分发
  2.  状态 API
  3.  空闲节点排空 -> 立即 drained=true
  4.  排空后新请求不进入该节点
  5.  有在途慢请求时排空 -> 等在途结束才完成，且原响应可取回
  6.  全部不可接流 -> 503
  7.  上游 5xx -> 不虚报成功、节点降级、在途归零
  8.  探测成功不解除排空（交错核心）
  9.  恢复不绕过故障探测（交错核心）
  10. 仅 GET：POST 转发路径返回 405
  11. /docs 可打开
  12. 排空等待超时 -> 202，在途数准确
  13. 上游挂起 -> 网关请求超时 504，不虚报成功、在途归零
  14. 多并发在途排空
  15. 未知节点管理接口 404
  16. 影子核对：same / different / 状态码与响应体摘要
  17. 影子核对：未知目标 / 同节点目标 -> uncomparable
  18. 影子请求计入目标在途，排空必须等影子结束（核对×排空交错核心）
  19. 排空后不再启动该节点的影子请求；resume 后恢复
  20. 主请求失败不得报相同（504 主失败 -> uncomparable）
  21. 影子目标超时 / 不可达 -> uncomparable，绝不能报 same
  22. 待完成记录占满 -> X-Compare-Skipped，主响应照常；终态后可恢复登记
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

# 优先使用环境变量 PYLIBS；否则自动发现本地 venv 的 site-packages
_DEFAULT_PYLIBS = "/tmp/pylibs"
for cand in ("/tmp/gwvenv/lib/python3.11/site-packages",):
    if os.path.isdir(cand):
        _DEFAULT_PYLIBS = cand
        break
PYLIBS = os.environ.get("PYLIBS", _DEFAULT_PYLIBS)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = sys.executable

GW_PORT = 19080
U1_PORT = 19081
U2_PORT = 19082
U3_PORT = 19085  # 仅在场景 20/21 作为额外健康影子目标接入

passed = 0
failed = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global passed, failed
    if cond:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        print(f"  [FAIL] {name} {detail}")


def http(method: str, url: str, timeout: float = 20.0, data: bytes | None = None,
         headers: dict | None = None):
    """返回 (status, headers, body_text)。4xx/5xx 不抛异常。"""
    req = urllib.request.Request(url, method=method, data=data, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read().decode()


def hget(url: str, headers: dict, timeout: float = 20.0):
    return http("GET", url, timeout=timeout, headers=headers)


def header(headers: dict, name: str):
    """大小写不敏感取响应头。"""
    lname = name.lower()
    for k, v in headers.items():
        if k.lower() == lname:
            return v
    return None


def wait_compare(cid: str, timeout: float = 10.0) -> dict:
    """轮询单条核对记录直到离开 pending。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/admin/compares/{cid}", timeout=5)
        assert status == 200, body
        rec = body
        if rec["status"] != "pending":
            return rec
        time.sleep(0.1)
    raise AssertionError(f"核对 {cid} 在 {timeout}s 内未完成")


def jget(url: str, timeout: float = 20.0):
    status, _, body = http("GET", url, timeout)
    return status, json.loads(body)


def wait_ready(url: str, timeout: float = 25.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, _, _ = http("GET", url, timeout=2)
            if status == 200:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def spawn(module_dir: str, port: int, extra_env: dict | None = None):
    # -I 隔离模式会忽略 PYTHONPATH 且不含 cwd，故用 runpy 包装器显式注入依赖与应用目录
    launcher = (
        "import sys,runpy,os;"
        f"sys.path[:0]=[{PYLIBS!r}, os.getcwd()];"
        "sys.argv=['uvicorn']+sys.argv[1:];"
        "runpy.run_module('uvicorn', run_name='__main__')"
    )
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env.update(extra_env or {})
    proc = subprocess.Popen(
        [PYTHON, "-I", "-c", launcher, "app:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=module_dir,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return proc


def kill(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        try:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=8)
        except Exception:
            proc.kill()


def node_status(node: str) -> dict:
    status, body = jget(f"http://127.0.0.1:{GW_PORT}/admin/status")
    assert status == 200
    match = next((n for n in body["nodes"] if n["name"] == node), None)
    assert match is not None, f"节点 {node} 不存在于当前网关: {[n['name'] for n in body['nodes']]}"
    return match


def wait_node(node: str, predicate, timeout: float = 8.0) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        last = node_status(node)
        if predicate(last):
            return last
        time.sleep(0.2)
    raise AssertionError(f"节点 {node} 等待条件超时: {last}")


def set_mode(port: int, mode: str) -> None:
    status, _, body = http("POST", f"http://127.0.0.1:{port}/control/mode?mode={mode}")
    assert status == 200, body


def main() -> int:
    procs = []
    try:
        print("== 启动两个上游和网关 ==")
        u1 = spawn(os.path.join(ROOT, "upstream"), U1_PORT, {"NODE_ID": "node1"})
        u2 = spawn(os.path.join(ROOT, "upstream"), U2_PORT, {"NODE_ID": "node2"})
        u3 = spawn(os.path.join(ROOT, "upstream"), U3_PORT, {"NODE_ID": "node3"})
        procs = [u1, u2, u3]
        assert wait_ready(f"http://127.0.0.1:{U1_PORT}/")
        assert wait_ready(f"http://127.0.0.1:{U2_PORT}/")
        assert wait_ready(f"http://127.0.0.1:{U3_PORT}/")

        gw_env = {
            "UPSTREAMS": f"node1=http://127.0.0.1:{U1_PORT},node2=http://127.0.0.1:{U2_PORT}",
            "PROBE_INTERVAL": "1",
            "PROBE_TIMEOUT": "1",
            "REQUEST_TIMEOUT": "15",
            "DRAIN_DEFAULT_TIMEOUT": "10",
        }
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, gw_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs"), "网关启动失败"
        time.sleep(1.2)  # 等首轮探测结果稳定

        # 1. 轮询分发
        print("== 1. 轮询分发 ==")
        seen = []
        for _ in range(4):
            status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
            assert status == 200, body
            seen.append(body["node"])
        check("4 个请求在两节点间交替", seen == ["node1", "node2", "node1", "node2"], str(seen))
        st = node_status("node1")
        check("rr_hits 各 2 次", st["rr_hits"] == 2, str(st))
        check("在途计数已归零", st["inflight"] == 0, str(st))

        # 2. 状态 API 内容
        print("== 2. 状态 API ==")
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/admin/status")
        check("两节点均健康", all(n["healthy"] for n in body["nodes"]))
        check("两节点均未排空", all(not n["draining"] for n in body["nodes"]))
        check("eligible 含两节点", sorted(body["eligible_nodes"]) == ["node1", "node2"])

        # 3. 空闲节点排空立即完成
        print("== 3. 空闲节点排空 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=5")
        b = json.loads(body)
        check("排空返回 200", status == 200, body)
        check("drained=true", b.get("drained") is True, body)
        check("inflight=0", b.get("inflight") == 0, body)
        st = node_status("node1")
        check("状态显示 draining", st["draining"] is True and st["eligible"] is False)

        # 4. 排空后新请求不进入 node1
        print("== 4. 排空后新请求只去 node2 ==")
        nodes = set()
        for _ in range(3):
            status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
            nodes.add(body["node"])
        check("新请求全部落在 node2", nodes == {"node2"}, str(nodes))

        # 5. 在途慢请求 + 排空交错：排空等在途结束，原响应可取回
        print("== 5. 在途慢请求期间排空 node2 ==")
        result = {}

        def slow_call():
            result["status"], _, result["body"] = http(
                "GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=3", timeout=20
            )

        t = threading.Thread(target=slow_call)
        t.start()
        # 等到请求确实进入 node2
        inflight_seen = 0
        deadline = time.time() + 5
        while time.time() < deadline:
            st2 = node_status("node2")
            inflight_seen = max(inflight_seen, st2["inflight"])
            if st2["inflight"] == 1:
                break
            time.sleep(0.1)
        check("慢请求已进入 node2 且 inflight=1", inflight_seen == 1, str(st2))

        drain_started = time.time()
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=10"
        )
        drain_elapsed = time.time() - drain_started
        b = json.loads(body)
        t.join(timeout=20)
        check("排空请求被在途请求阻塞约 3s", drain_elapsed >= 2.5, f"{drain_elapsed:.2f}s")
        check("排空最终成功 drained=true", status == 200 and b.get("drained") is True, body)
        check("慢请求原响应成功取回 200", result.get("status") == 200, str(result))
        check("响应来自 node2 且内容完整",
              json.loads(result["body"]).get("node") == "node2"
              and json.loads(result["body"]).get("slept", 0) >= 2.5,
              result.get("body"))
        check("结束后两节点在途均为 0",
              all(node_status(n)["inflight"] == 0 for n in ("node1", "node2")))

        # 6. 两节点都排空 -> 503
        print("== 6. 无可用节点返回 503 ==")
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("返回 503", status == 503, body)
        check("503 原因可解释", json.loads(body).get("error") == "no_eligible_upstream", body)

        # 7. 上游 5xx：不虚报成功 + 节点降级 + 在途归零
        print("== 7. 上游故障 (5xx) ==")
        # 先恢复 node1 以便发请求（node2 仍排空）
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        assert status == 200
        set_mode(U1_PORT, "error")
        time.sleep(1.3)  # 等周期性探测先发现故障
        st = node_status("node1")
        check("探测已将 node1 标记为不健康", st["healthy"] is False, str(st))
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky")
        check("无健康节点时返回 503 而非虚假 200", status == 503, body)
        # 临时恢复健康标志位才能真正打到 5xx（模拟请求路径上的失败降级）
        set_mode(U1_PORT, "healthy")
        time.sleep(1.3)
        st = node_status("node1")
        check("探测恢复后 node1 健康", st["healthy"] is True, str(st))
        set_mode(U1_PORT, "error")  # 不等待探测，让请求先撞上 5xx
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky")
        check("5xx 状态如实返回", status == 500, f"status={status} body={body}")
        time.sleep(0.3)
        st = node_status("node1")
        check("请求失败后节点即时降级", st["healthy"] is False, str(st))
        check("失败后在途计数仍归零", st["inflight"] == 0, str(st))

        # 8. 交错核心：探测成功不得解除排空
        print("== 8. 探测成功不解除排空 ==")
        set_mode(U1_PORT, "healthy")
        # node1 当前未排空，先排空，再让周期性探测持续成功
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=3")
        assert status == 200
        time.sleep(2.5)  # 跨越至少 2 轮探测，上游为 healthy，探测必然成功
        st = node_status("node1")
        check("多轮探测成功后排空标志仍在", st["draining"] is True, str(st))
        check("探测成功只更新健康位", st["healthy"] is True and st["eligible"] is False, str(st))

        # 9. 交错核心：恢复不绕过故障探测
        print("== 9. 恢复不绕过故障探测 ==")
        set_mode(U1_PORT, "error")
        time.sleep(1.3)  # 探测确认故障
        st_before = node_status("node1")
        check("恢复前 node1 不健康", st_before["healthy"] is False, str(st_before))
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        b = json.loads(body)
        check("resume 清除排空但节点仍不可接流",
              status == 200 and b["draining"] is False and b["eligible"] is False, body)
        st = node_status("node1")
        check("draining=False 且 healthy=False",
              st["draining"] is False and st["healthy"] is False, str(st))
        # 故障节点上请求不会被分配：node2 仍排空 -> 503
        status, _, _ = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("故障节点不会被轮询选中 (503)", status == 503)
        # 上游恢复后经探测自动回到可接流
        set_mode(U1_PORT, "healthy")
        time.sleep(1.5)
        st = node_status("node1")
        check("探测恢复后节点重新 eligible", st["eligible"] is True, str(st))
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("流量恢复进入 node1", status == 200 and body["node"] == "node1")

        # 10. 仅 GET 转发
        print("== 10. 非 GET 方法被拒 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/proxy/echo", data=b"x")
        check("POST 转发路径返回 405", status == 405, f"status={status}")

        # 11. /docs 可打开
        print("== 11. /docs 与 openapi.json ==")
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/docs")
        check("/docs 返回 200", status == 200 and "swagger" in body.lower())
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/openapi.json")
        paths = body["paths"]
        check("openapi 含管理与转发接口",
              "/admin/nodes/{name}/drain" in paths and "/proxy/{path}" in paths)

        # 12. 排空等待超时：202 + 在途准确
        print("== 12. 排空超时返回 202 ==")
        result12 = {}

        def slow_call12():
            result12["status"], _, result12["body"] = http(
                "GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=4", timeout=15
            )

        t12 = threading.Thread(target=slow_call12)
        t12.start()
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node1")["inflight"] != 1:
            time.sleep(0.1)
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=1"
        )
        b = json.loads(body)
        check("短超时排空返回 202", status == 202, body)
        check("drained=false 且 inflight=1", b.get("drained") is False and b.get("inflight") == 1, body)
        t12.join(timeout=15)
        check("在途慢请求仍拿到完整 200", result12.get("status") == 200, str(result12))
        time.sleep(0.3)
        # 在途归零后再次排空（节点已在排空中）应立即完成
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=3")
        b = json.loads(body)
        check("在途归零后排空立即完成", status == 200 and b["drained"] is True, body)

        # 13. 请求超时：重启网关为短超时，上游 hang（探测与业务请求都会挂住）
        print("== 13. 上游挂起 -> 网关 504 超时 ==")
        kill(gw)
        procs.remove(gw)
        short_env = dict(gw_env)
        short_env["REQUEST_TIMEOUT"] = "1"
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, short_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        set_mode(U1_PORT, "healthy")
        # 等首轮探测把 node1 标记为健康可接流（网关重启后状态全清）
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node1")["eligible"]:
            time.sleep(0.2)
        check("重启后 node1 经探测恢复可接流", node_status("node1")["eligible"] is True)
        # 切 hang 后立即发请求：赶在下一轮周期探测摘掉它之前验证网关自身的请求超时
        set_mode(U1_PORT, "hang")
        t0 = time.time()
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky", timeout=8)
        elapsed = time.time() - t0
        if status == 503:
            # 探测抢先把节点摘掉，则手动恢复后重试一次，确保覆盖请求超时路径
            set_mode(U1_PORT, "healthy")
            deadline = time.time() + 6
            while time.time() < deadline and not node_status("node1")["eligible"]:
                time.sleep(0.2)
            set_mode(U1_PORT, "hang")
            t0 = time.time()
            status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky", timeout=8)
            elapsed = time.time() - t0
        check("挂起请求在约 1s 内返回 504", status == 504 and elapsed < 4,
              f"status={status} elapsed={elapsed:.1f}s body={body}")
        check("未虚报成功", status != 200)
        time.sleep(0.3)
        st = node_status("node1")
        check("超时后在途计数准确归零", st["inflight"] == 0, str(st))
        check("超时后节点被标记不健康", st["healthy"] is False, str(st))
        set_mode(U1_PORT, "healthy")

        # 14. 多个并发在途请求：排空须等全部结束，在途计数全程准确
        print("== 14. 多并发在途请求排空 ==")
        # 恢复正常请求超时并重启网关（清空场景 13 的短超时与节点状态）
        kill(gw)
        procs.remove(gw)
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, gw_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node1")["eligible"]:
            time.sleep(0.2)
        check("重启后 node1 可接流", node_status("node1")["eligible"] is True)
        # 先排空 node2，把流量集中到 node1
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=5")
        assert status == 200
        results14 = []

        def slow_call14(idx: int):
            s, _, b = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=3", timeout=20)
            results14.append((idx, s, b))

        threads = [threading.Thread(target=slow_call14, args=(i,)) for i in range(4)]
        for th in threads:
            th.start()
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node1")["inflight"] < 4:
            time.sleep(0.1)
        st = node_status("node1")
        check("4 个在途请求计数准确", st["inflight"] == 4, str(st))
        t0 = time.time()
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=15"
        )
        elapsed = time.time() - t0
        b = json.loads(body)
        for th in threads:
            th.join(timeout=20)
        check("排空阻塞到全部 4 个在途请求结束", elapsed >= 2.5 and b.get("drained") is True,
              f"elapsed={elapsed:.1f}s body={body}")
        check("4 个慢请求全部取回完整 200",
              len(results14) == 4 and all(s == 200 for _, s, _ in results14),
              str(results14))
        check("全部结束后在途归零", node_status("node1")["inflight"] == 0)

        # 15. 未知节点管理接口返回 404
        print("== 15. 未知节点 404 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/ghost/drain")
        check("排空未知节点返回 404", status == 404, body)
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/ghost/resume")
        check("恢复未知节点返回 404", status == 404, body)

        # ---------- 影子核对 ----------
        # 先恢复两个节点到健康可接流的干净状态
        print("== 准备：恢复两节点 ==")
        for nd in ("node1", "node2"):
            st = node_status(nd)
            if st["draining"]:
                status, _, _ = http(
                    "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/{nd}/resume"
                )
                assert status == 200
        deadline = time.time() + 8
        while time.time() < deadline and not all(
            node_status(n)["eligible"] for n in ("node1", "node2")
        ):
            time.sleep(0.2)
        check("两节点均 eligible",
              all(node_status(n)["eligible"] for n in ("node1", "node2")))

        # 16. 影子核对：same / different / 摘要与编号
        print("== 16. 影子核对 same / different ==")
        # 双节点严格轮询交替：先发一个探测请求得知落点 X，紧接着的真实请求
        # 必然落在另一节点，因此把影子目标设为 X，保证主、影子跨节点
        status, probe = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        assert status == 200
        target16 = probe["node"]
        # /same 与节点身份、请求头均无关 -> 两节点响应字节一致 -> same
        status, hdrs, body = hget(
            f"http://127.0.0.1:{GW_PORT}/proxy/same?a=1&b=2",
            headers={"X-Compare-With": target16},
        )
        check("主请求成功 200", status == 200, body)
        cid = header(hdrs, "X-Compare-Id")
        check("响应头返回核对编号", bool(cid), str(hdrs))
        check("未被跳过", header(hdrs, "X-Compare-Skipped") is None, str(hdrs))
        rec = wait_compare(cid)
        expected_primary = "node2" if target16 == "node1" else "node1"
        check("主、影子分属两个节点",
              rec["primary_node"] == expected_primary and rec["target_node"] == target16,
              json.dumps(rec, ensure_ascii=False))
        check("相同路径与查询串", rec["path"] == "same" and rec["query"] == "a=1&b=2",
              f"{rec['path']}?{rec['query']}")
        check("判定 same", rec["status"] == "same", json.dumps(rec, ensure_ascii=False))
        check("双方状态码均 200",
              rec["primary"]["status_code"] == 200 and rec["shadow"]["status_code"] == 200,
              json.dumps(rec, ensure_ascii=False))
        check("记录含双方响应体摘要",
              bool(rec["primary"]["body_preview"])
              and rec["primary"]["content_length"] > 0
              and rec["shadow"]["content_length"] == rec["primary"]["content_length"],
              json.dumps(rec, ensure_ascii=False))
        check("目标节点影子计数累计",
              node_status(target16)["shadow_count"] >= 1, json.dumps(rec, ensure_ascii=False))
        # 列表接口可见该记录
        status, listing = jget(f"http://127.0.0.1:{GW_PORT}/admin/compares")
        check("列表接口含该编号与 same 计数",
              status == 200
              and any(r["id"] == cid for r in listing["records"])
              and listing["counts"]["same"] >= 1
              and listing["max_records"] == 100,
              json.dumps(listing)[:300])

        # /echo 回显节点身份与请求头 -> 两节点响应体不同 -> different，
        # 同时验证影子请求确实携带 X-Shadow-Compare 标识
        status, probe = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        target16b = probe["node"]
        status, hdrs, body = hget(
            f"http://127.0.0.1:{GW_PORT}/proxy/echo?q=hello",
            headers={"X-Compare-With": target16b},
        )
        cid2 = header(hdrs, "X-Compare-Id")
        rec2 = wait_compare(cid2)
        check("不同内容判定 different", rec2["status"] == "different",
              json.dumps(rec2, ensure_ascii=False))
        check("different 记录附双方摘要",
              rec2["primary"]["status_code"] == 200
              and rec2["shadow"]["status_code"] == 200
              and f'"node":"{target16b}"' in rec2["shadow"]["body_preview"],
              json.dumps(rec2, ensure_ascii=False))
        # 预览在 256 字节处截断，核对 id 可能不完整，故只验证影子头字段存在；
        # 影子任务传入的 id 与主响应 X-Compare-Id 同源，终态归属已由本记录证明
        check("影子请求携带核对标识 X-Shadow-Compare",
              "x-shadow-compare" in rec2["shadow"]["body_preview"],
              rec2["shadow"]["body_preview"])
        check("查询串透传一致", rec2["query"] == "q=hello", rec2["query"])

        # 17. 未知目标 / 同节点目标 -> uncomparable
        print("== 17. 无法核对的目标 ==")
        status, hdrs, _ = hget(
            f"http://127.0.0.1:{GW_PORT}/proxy/same",
            headers={"X-Compare-With": "ghost"},
        )
        rec = wait_compare(header(hdrs, "X-Compare-Id"))
        check("未知目标 -> uncomparable/unknown_target",
              rec["status"] == "uncomparable" and rec["reason"] == "unknown_target",
              json.dumps(rec, ensure_ascii=False))

        # 探测落点 X，下一真实请求主节点为对端 Y；把目标设为 Y 即与主节点相同
        status, probe = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        same_target = "node2" if probe["node"] == "node1" else "node1"
        status, hdrs, _ = hget(
            f"http://127.0.0.1:{GW_PORT}/proxy/same",
            headers={"X-Compare-With": same_target},
        )
        rec = wait_compare(header(hdrs, "X-Compare-Id"))
        check("同节点目标 -> uncomparable/same_node_as_primary",
              rec["status"] == "uncomparable"
              and rec["reason"] == "same_node_as_primary"
              and rec["primary_node"] == same_target
              and rec["target_node"] == same_target,
              json.dumps(rec, ensure_ascii=False))
        check("uncomparable 不产生 shadow 侧摘要", rec["shadow"] is None,
              json.dumps(rec, ensure_ascii=False))
        check("单条查询未知编号 -> 404",
              http("GET", f"http://127.0.0.1:{GW_PORT}/admin/compares/nope")[0] == 404)

        # 18. 核对 × 排空交错：影子计入目标在途，排空必须等影子结束
        print("== 18. 影子在途阻塞排空 ==")
        # 探测落点 X；紧随的慢请求主节点为对端 Y，影子目标设为 X（跨节点）
        status, probe = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        target18 = probe["node"]
        primary18 = "node2" if target18 == "node1" else "node1"

        shadow_result = {}

        def shadowed_slow():
            s, h, b = hget(
                f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=4",
                headers={"X-Compare-With": target18},
                timeout=20,
            )
            shadow_result["status"], shadow_result["headers"], shadow_result["body"] = s, h, b

        th = threading.Thread(target=shadowed_slow)
        th.start()
        # 主请求与影子任务异步派发：分别记录两节点在途峰值。
        # 主请求约 4s，期间持续轮询即可捕获影子在途（影子同样睡眠 4s）。
        saw_primary = False
        saw_target = False
        deadline = time.time() + 8
        while time.time() < deadline:
            st_p, st_t = node_status(primary18), node_status(target18)
            saw_primary = saw_primary or st_p["inflight"] >= 1
            saw_target = saw_target or st_t["inflight"] >= 1
            if saw_primary and saw_target:
                break
            if not th.is_alive():
                # 主请求已返回：再给影子任务一点派发时间后退出轮询
                time.sleep(0.3)
                st_p, st_t = node_status(primary18), node_status(target18)
                saw_target = saw_target or st_t["inflight"] >= 1
                break
            time.sleep(0.03)
        check("主请求曾在主节点在途", saw_primary)
        check("影子请求曾在目标节点在途", saw_target,
              f"{target18} 未见 inflight，shadow_count={node_status(target18)['shadow_count']}")
        st_t = node_status(target18)
        check("目标节点影子计数累计", st_t["shadow_count"] >= 1, str(st_t))

        drain_started = time.time()
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/{target18}/drain?timeout=12"
        )
        drain_elapsed = time.time() - drain_started
        b = json.loads(body)
        th.join(timeout=20)
        check("排空被影子在途阻塞到约 4s", drain_elapsed >= 3.0, f"{drain_elapsed:.2f}s")
        check("排空最终 drained=true（未提前报可停机）",
              status == 200 and b.get("drained") is True, body)
        check("主响应照常及时返回 200", shadow_result.get("status") == 200,
              str(shadow_result))
        cid = header(shadow_result["headers"], "X-Compare-Id")
        rec = wait_compare(cid)
        check("核对最终拿到终态（same/different 之一，非 pending）",
              rec["status"] in ("same", "different"), json.dumps(rec, ensure_ascii=False))
        check("结束后在途归零",
              all(node_status(n)["inflight"] == 0 for n in ("node1", "node2")))

        # 19. 排空后不能再启动该节点的影子请求
        print("== 19. 排空节点不承接新影子请求 ==")
        # target18 在场景 18 末尾仍处于排空；主请求只能走另一节点，核对目标指向它
        check("场景 18 目标仍在排空", node_status(target18)["draining"] is True)
        status, hdrs, body = hget(
            f"http://127.0.0.1:{GW_PORT}/proxy/same",
            headers={"X-Compare-With": target18},
        )
        check("主响应不受影响仍 200", status == 200, body)
        rec = wait_compare(header(hdrs, "X-Compare-Id"))
        check("排空目标 -> uncomparable/target_draining",
              rec["status"] == "uncomparable" and rec["reason"] == "target_draining",
              json.dumps(rec, ensure_ascii=False))
        sc_before = node_status(target18)["shadow_count"]
        time.sleep(0.5)
        check("排空后影子计数不增加", node_status(target18)["shadow_count"] == sc_before)

        # 不健康目标同样不可核对：先 resume（去掉 draining 因素），再置故障，
        # 使节点处于「健康位 false 且未排空」，独立验证 target_unhealthy 分支
        target_port = U2_PORT if target18 == "node2" else U1_PORT
        status, _, _ = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/{target18}/resume"
        )
        assert status == 200
        set_mode(target_port, "error")
        time.sleep(1.5)
        st_t = node_status(target18)
        check(f"探测已将 {target18} 标记不健康且未排空",
              st_t["healthy"] is False and st_t["draining"] is False, str(st_t))
        status, hdrs, _ = hget(
            f"http://127.0.0.1:{GW_PORT}/proxy/same",
            headers={"X-Compare-With": target18},
        )
        rec = wait_compare(header(hdrs, "X-Compare-Id"))
        check("不健康目标 -> uncomparable/target_unhealthy",
              rec["status"] == "uncomparable" and rec["reason"] == "target_unhealthy",
              json.dumps(rec, ensure_ascii=False))
        set_mode(target_port, "healthy")
        status, _, _ = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/{target18}/resume"
        )
        assert status == 200
        deadline = time.time() + 8
        while time.time() < deadline and not node_status(target18)["eligible"]:
            time.sleep(0.2)
        check(f"resume 后 {target18} 重新承接影子", node_status(target18)["eligible"])

        # 20. 主请求失败不得报相同：短超时网关 + 上游 hang
        print("== 20. 主请求失败不得报 same ==")
        kill(gw)
        procs.remove(gw)
        fail_env = dict(gw_env)
        fail_env["REQUEST_TIMEOUT"] = "1"
        fail_env["COMPARE_TIMEOUT"] = "8"
        # 接入第三个健康节点 node3 作为影子目标：node2 排空后主流量锁定 node1，
        # node3 保持健康可承接影子，从而能真正走到「主失败」分支
        fail_env["UPSTREAMS"] = (
            f"node1=http://127.0.0.1:{U1_PORT},node2=http://127.0.0.1:{U2_PORT},"
            f"node3=http://127.0.0.1:{U3_PORT}"
        )
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, fail_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        for nd in ("node1", "node2", "node3"):
            wait_node(nd, lambda n: n["eligible"])
        # 排空 node2：主请求在 node1/node3 间轮询，node3 保持健康作为影子目标
        status, _, _ = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=5"
        )
        assert status == 200
        # node1 置 hang：主请求落到 node1 时 504，落到 node3 时 200。
        # 重试直到命中 hang 的 node1，验证主失败分支（影子始终打健康的 node3）
        set_mode(U1_PORT, "hang")
        t0 = time.time()
        status = 0
        hdrs = {}
        body = ""
        deadline = time.time() + 12
        while time.time() < deadline:
            status, hdrs, body = hget(
                f"http://127.0.0.1:{GW_PORT}/proxy/flaky",
                headers={"X-Compare-With": "node3"},
                timeout=8,
            )
            if status == 504:
                break
            time.sleep(0.3)
        elapsed = time.time() - t0
        check("主请求命中 hang 的 node1 并约 1s 返回 504",
              status == 504 and elapsed < 12,
              f"status={status} elapsed={elapsed:.1f}s body={body}")
        rec = wait_compare(header(hdrs, "X-Compare-Id"))
        check("主失败 -> uncomparable 且原因含 primary_failed",
              rec["status"] == "uncomparable"
              and (rec["reason"] or "").startswith("primary_failed:timeout"),
              json.dumps(rec, ensure_ascii=False))
        check("主失败记录没有 shadow 侧内容", rec["shadow"] is None)
        set_mode(U1_PORT, "healthy")

        # 21. 影子目标超时 / 不可达 -> uncomparable，绝不能报 same
        print("== 21. 影子超时 / 不可达 ==")
        # 重启网关：主超时放宽，影子超时缩短到 1s
        kill(gw)
        procs.remove(gw)
        sh_env = dict(gw_env)
        sh_env["REQUEST_TIMEOUT"] = "12"
        sh_env["COMPARE_TIMEOUT"] = "1"
        # 第三个上游指向一个无人监听的端口，用于确定性验证「影子不可达」
        dead_port = 19083
        sh_env["UPSTREAMS"] = (
            f"node1=http://127.0.0.1:{U1_PORT},node2=http://127.0.0.1:{U2_PORT},"
            f"node3=http://127.0.0.1:{dead_port}"
        )
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, sh_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        deadline = time.time() + 8
        while time.time() < deadline and not all(
            node_status(n)["eligible"] for n in ("node1", "node2")
        ):
            time.sleep(0.2)

        # 21a. 影子超时（确定性）：shadow_delay 仅对带 X-Shadow-Compare 的影子生效，
        #      主请求不带该头因此立即返回，影子在目标节点睡眠 3s 后被 1s 超时截断
        status, probe = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        target21 = probe["node"]  # 下一真实请求落在另一节点，目标设为探测落点
        t0 = time.time()
        status, hdrs, body = hget(
            f"http://127.0.0.1:{GW_PORT}/proxy/same?shadow_delay=3",
            headers={"X-Compare-With": target21},
            timeout=12,
        )
        primary_elapsed = time.time() - t0
        check("主请求不受影子拖累、及时 200",
              status == 200 and primary_elapsed < 3, f"{status} {primary_elapsed:.1f}s")
        rec = wait_compare(header(hdrs, "X-Compare-Id"), timeout=8)
        check("影子超时 -> uncomparable/shadow_timeout，绝不报 same",
              rec["status"] == "uncomparable"
              and (rec["reason"] or "").startswith("shadow_timeout"),
              json.dumps(rec, ensure_ascii=False))
        check("保留主侧摘要", rec["primary"] is not None
              and rec["primary"]["status_code"] == 200)
        deadline = time.time() + 6
        while time.time() < deadline and any(
            node_status(n)["inflight"] for n in ("node1", "node2")
        ):
            time.sleep(0.1)
        check("影子超时后两节点在途归零",
              all(node_status(n)["inflight"] == 0 for n in ("node1", "node2")))

        # 21b. 影子不可达（确定性）：node3 指向关闭端口，探测标记为不健康，
        #      先验证健康门槛拦截；再临时通过不可达地址直接验证连接错误路径
        st3 = node_status("node3")
        check("死端口节点经探测标记不健康", st3["healthy"] is False, str(st3))
        status, hdrs, _ = hget(
            f"http://127.0.0.1:{GW_PORT}/proxy/same",
            headers={"X-Compare-With": "node3"},
            timeout=10,
        )
        check("主请求仍 200", status == 200)
        rec = wait_compare(header(hdrs, "X-Compare-Id"))
        check("不健康目标 -> uncomparable/target_unhealthy",
              rec["status"] == "uncomparable"
              and rec["reason"] == "target_unhealthy",
              json.dumps(rec, ensure_ascii=False))

        # 21c. 影子连接错误路径：网关先探测到 node3 健康（指向常驻 u3），随后关掉
        #      u3，使节点 healthy=True 但连接立即被拒，确定性触发 shadow_unreachable
        # 先关掉场景 21 的旧网关，释放 GW_PORT，避免新旧进程抢同一端口
        kill(gw)
        procs.remove(gw)
        un_env = dict(gw_env)  # gw_env 的 UPSTREAMS 只含 node1/node2，无死端口
        un_env["COMPARE_TIMEOUT"] = "5"
        un_env["UPSTREAMS"] = (
            f"node1=http://127.0.0.1:{U1_PORT},node3=http://127.0.0.1:{U3_PORT}"
        )
        gw_un = spawn(os.path.join(ROOT, "gateway"), GW_PORT, un_env)
        procs.append(gw_un)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        # 确认连到的是新网关：其 node3 必须指向常驻 u3 端口而非旧的死端口
        deadline = time.time() + 8
        while time.time() < deadline:
            st = node_status("node3")
            if st["base_url"] == f"http://127.0.0.1:{U3_PORT}":
                break
            time.sleep(0.2)
        check("21c 网关 node3 指向常驻 u3",
              node_status("node3")["base_url"] == f"http://127.0.0.1:{U3_PORT}",
              node_status("node3")["base_url"])
        wait_node("node1", lambda n: n["eligible"])
        wait_node("node3", lambda n: n["eligible"])
        check("node3 探测健康可作为影子目标", node_status("node3")["eligible"])
        kill(u3)  # 关闭上游：健康标志仍为 True，但连接立即被拒
        procs.remove(u3)
        status, hdrs, body = hget(
            f"http://127.0.0.1:{GW_PORT}/proxy/same",
            headers={"X-Compare-With": "node3"},
            timeout=12,
        )
        check("主请求仍 200", status == 200, body)
        rec = wait_compare(header(hdrs, "X-Compare-Id"), timeout=10)
        check("影子连接被拒 -> uncomparable/shadow_unreachable，绝不报 same",
              rec["status"] == "uncomparable"
              and (rec["reason"] or "").startswith("shadow_unreachable"),
              json.dumps(rec, ensure_ascii=False))
        kill(gw_un)
        procs.remove(gw_un)

        # 22. 待完成记录占满 -> 跳过并标明，主响应照常；终态后恢复
        print("== 22. 记录占满跳过 ==")
        # 21c 的 gw_un 已在上一步 kill 并从 procs 移除，端口已释放
        cap_env = dict(gw_env)
        cap_env["COMPARE_MAX_RECORDS"] = "3"
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, cap_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        deadline = time.time() + 8
        while time.time() < deadline and not all(
            node_status(n)["eligible"] for n in ("node1", "node2")
        ):
            time.sleep(0.2)
        # 5 个并发慢核对（影子 3s），容量仅 3：前 3 条 pending 即占满，
        # 后续核对在整个慢请求窗口内都只能跳过
        cap_results = []

        def cap_call(i: int):
            s, h, b = hget(
                f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=3",
                headers={"X-Compare-With": "node2"},
                timeout=20,
            )
            cap_results.append((i, s, h, b))

        threads = [threading.Thread(target=cap_call, args=(i,)) for i in range(5)]
        for th in threads:
            th.start()
        # 待 3 条 pending 就位（容量占满）
        deadline = time.time() + 8
        while time.time() < deadline:
            _, listing = jget(f"http://127.0.0.1:{GW_PORT}/admin/compares")
            if listing["counts"]["pending"] >= 3:
                break
            time.sleep(0.05)
        # 第 6 个核对：记录占满且全 pending -> 跳过，但主响应照常
        status, hdrs, body = hget(
            f"http://127.0.0.1:{GW_PORT}/proxy/same",
            headers={"X-Compare-With": "node2"},
            timeout=10,
        )
        check("占满时主响应仍 200", status == 200, body)
        check("响应头标明跳过",
              header(hdrs, "X-Compare-Skipped") == "1"
              and header(hdrs, "X-Compare-Skip-Reason") == "store_full_pending",
              str(hdrs))
        check("跳过时不发核对编号", header(hdrs, "X-Compare-Id") is None)
        status, listing = jget(f"http://127.0.0.1:{GW_PORT}/admin/compares")
        check("跳过计数 +1 且存储不超上限",
              listing["skipped_full"] >= 1 and listing["stored"] == 3,
              json.dumps(listing)[:200])
        for th in threads:
            th.join(timeout=20)
        check("5 个慢请求主响应全部 200",
              len(cap_results) == 5 and all(r[1] == 200 for r in cap_results),
              str([r[1] for r in cap_results]))
        # 等记录全部终态后，名额释放，可重新登记
        deadline = time.time() + 10
        while time.time() < deadline:
            _, listing = jget(f"http://127.0.0.1:{GW_PORT}/admin/compares")
            if listing["counts"]["pending"] == 0:
                break
            time.sleep(0.2)
        status, hdrs, _ = hget(
            f"http://127.0.0.1:{GW_PORT}/proxy/same",
            headers={"X-Compare-With": "node2"},
            timeout=10,
        )
        check("终态淘汰名额后恢复登记", header(hdrs, "X-Compare-Id") is not None, str(hdrs))
        rec = wait_compare(header(hdrs, "X-Compare-Id"))
        check("恢复后的核对正常出结论", rec["status"] in ("same", "different"),
              json.dumps(rec, ensure_ascii=False))

        # 恢复默认网关配置，避免影响后续（本场景为最后一组，无需再重启）
        print(f"\n==== 结果: {passed} passed, {failed} failed ====")
        return 1 if failed else 0

    finally:
        for p in procs:
            kill(p)


if __name__ == "__main__":
    sys.exit(main())
