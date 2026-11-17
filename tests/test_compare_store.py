#!/usr/bin/env python3
"""核对记录存储（CompareStore）的纯逻辑自测，不依赖网络与子进程。

覆盖：
  1. 达到上限后，新记录淘汰最旧的已完成记录，总量不超上限；
  2. 全部为待完成记录时，新核对被跳过并计数，待完成数量不再增加；
  3. 跳过之后完成一条记录，可重新登记，计数与 stored 始终自洽；
  4. 列表顺序为最新在前，计数分桶正确。
"""
from __future__ import annotations

import asyncio
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 允许以 `python3 tests/test_compare_store.py` 直接运行（本地 venv 时）
sys.path.insert(0, os.path.join(ROOT, "gateway"))

from app import (  # noqa: E402
    COMPARE_MAX_RECORDS,
    STATUS_DIFFERENT,
    STATUS_PENDING,
    STATUS_SAME,
    STATUS_UNCOMPARABLE,
    CompareStore,
)

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


async def main() -> int:
    limit = COMPARE_MAX_RECORDS
    print(f"== 存储上限 = {limit} ==")

    # 1. 已完成记录在容量不足时按最旧优先淘汰，总量不超上限
    store = CompareStore(max_records=limit)
    finished_recs = []
    for i in range(60):
        rec = await store.create(
            path=f"/done{i}", query="", primary_node="n1", target_node="n2"
        )
        assert rec is not None
        await store.finish(rec, STATUS_SAME)
        finished_recs.append(rec)
    snap = await store.list_snapshot()
    check("60 条已完成记录全部保留", snap["stored"] == 60, str(snap["stored"]))

    for i in range(60, limit):  # 再填 40 条 pending，达到上限
        rec = await store.create(
            path=f"/pend{i}", query="", primary_node="n1", target_node="n2"
        )
        if rec is None:
            check(f"第 {i + 1} 条可登记", False)
            break
    snap = await store.list_snapshot()
    check("填到上限 stored=100", snap["stored"] == limit, str(snap["stored"]))

    # 再追加：存在已完成记录时先淘汰最旧的已完成记录，而不是跳过
    rec = await store.create(
        path="/evict-oldest", query="", primary_node="n1", target_node="n2"
    )
    check("有已完成记录时淘汰最旧记录而非跳过", rec is not None and rec.path == "/evict-oldest")
    check("最旧的已完成记录被淘汰", await store.get(finished_recs[0].id) is None)
    check("较新的已完成记录仍在", await store.get(finished_recs[-1].id) is not None)
    check("淘汰后 stored 仍为上限", (await store.list_snapshot())["stored"] == limit)

    # 2. 全部待完成 -> 新核对跳过（重新构造一个全 pending 的存储）
    store = CompareStore(max_records=limit)
    for i in range(limit):
        rec = await store.create(
            path=f"/full{i}", query="", primary_node="n1", target_node="n2"
        )
        assert rec is not None, i
    snap = await store.list_snapshot()
    check("全 pending 填满 stored=100",
          snap["stored"] == limit and snap["counts"][STATUS_PENDING] == limit,
          str(snap))

    rec = await store.create(
        path="/overflow", query="", primary_node="n1", target_node="n2"
    )
    check("待完成占满时 create 返回 None", rec is None)
    snap = await store.list_snapshot()
    check("跳过后 stored 仍为 100", snap["stored"] == limit, str(snap["stored"]))
    check("跳过计数 +1", snap["skipped_full"] == 1, str(snap["skipped_full"]))
    check("100 条全部 pending", snap["counts"][STATUS_PENDING] == limit, str(snap["counts"]))

    rec2 = await store.create(
        path="/overflow2", query="", primary_node="n1", target_node="n2"
    )
    check("再次跳过仍返回 None", rec2 is None)
    check("跳过计数累加 = 2", (await store.list_snapshot())["skipped_full"] == 2)

    # 3. 完成一条（最早的 pending）后可重新登记；新记录是 pending，
    #    会顶掉那条唯一的已完成记录，因此存储重新回到全 pending
    oldest_pending = (await store.list_snapshot())["records"][-1]
    target_rec = await store.get(oldest_pending["id"])
    await store.finish(target_rec, STATUS_DIFFERENT)
    mid_snap = await store.list_snapshot()
    check("完成后出现 1 条 different",
          mid_snap["counts"][STATUS_DIFFERENT] == 1
          and mid_snap["counts"][STATUS_PENDING] == limit - 1,
          str(mid_snap["counts"]))
    rec3 = await store.create(
        path="/after-finish", query="", primary_node="n1", target_node="n2"
    )
    check("腾出已完成名额后可登记", rec3 is not None and rec3.path == "/after-finish")
    snap = await store.list_snapshot()
    check("登记后 stored 仍为上限", snap["stored"] == limit, str(snap["stored"]))
    check(
        "新 pending 顶掉终态记录后回到全 pending",
        snap["counts"][STATUS_PENDING] == limit
        and snap["counts"][STATUS_DIFFERENT] == 0
        and snap["counts"][STATUS_SAME] == 0
        and snap["counts"][STATUS_UNCOMPARABLE] == 0,
        str(snap["counts"]),
    )

    # 4. 最新在前；get 命中 / 未命中
    listing = (await store.list_snapshot())["records"]
    check("列表最新在前", listing[0]["path"] == "/after-finish", listing[0]["path"])
    check("get 命中最新记录", (await store.get(rec3.id)) is not None)
    check("get 未知 id 返回 None", await store.get("nope") is None)

    print(f"\n==== 结果: {passed} passed, {failed} failed ====")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
