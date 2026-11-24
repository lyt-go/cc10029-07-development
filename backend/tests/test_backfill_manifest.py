"""补传链路集成测试：交叠窗口缺失筛选、清单领用独占、差量分片与归属回查。

覆盖题述场景：
- 按闭合窗口摘要查询缺失记录并保存为待补传数据清单；
- 两个相邻窗口交界处同一条更新记录，后一窗先领用，前一窗在筛选确认后收到更正，
  同一源记录本轮只可由一个窗口发送；
- 已确认清单可回查引用的窗口版本与已发出范围，旧清单重新打开后引用仍可解释；
- 传输批次领取清单生成差量分片，对照分片可查交叠记录归属与待补记录未发送原因。
"""
from __future__ import annotations

import time
from pathlib import Path

import pytest

import app.services.base as base_module
from app.services import base as base_pkg
from app.services.backfill_manifest import BackfillManifestService
from app.services.extract_window import ExtractWindowService
from app.services.source_register import SourceRegisterService
from app.services.transfer_batch import TransferBatchService
from app.routers import backfill as backfill_router
from app.store import store


@pytest.fixture(autouse=True)
def isolated_data_root(tmp_path, monkeypatch):
    """把版本化产物目录隔离到临时路径，避免 append 型 jsonl 跨测试累积。"""
    monkeypatch.setattr(base_module, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(base_pkg, "DATA_ROOT", tmp_path)
    yield


@pytest.fixture(autouse=True)
def reset_store():
    from app.seed import SEED_ROWS
    store._tables = {
        name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
    }
    yield


def _make_source(code: str = "SOUR-BF") -> None:
    svc = SourceRegisterService()
    entry = svc.register_source({
        "源端编号": code, "系统名称": "补传测试源", "数据域": "订单域",
        "记录格式": "JSON", "编码方式": "UTF-8", "日增量级": "10万",
        "抽取方式": "增量抽取", "登记游标": "0",
    })
    svc.confirm_register(entry["id"])


def _closed_window(source: str, label: str, record_keys: list[str]) -> tuple[int, str]:
    """创建并闭合一个窗口，直接写入带稳定 record_key 的抽取产物与闭合摘要。"""
    ew = ExtractWindowService()
    window = ew.create_window(source, count=len(record_keys))
    window_code = window["窗口编号"]
    artifact = base_module.DATA_ROOT / "extracted" / f"{window_code}.jsonl"
    records = [
        {"record_key": key, "batch_key": window_code, "line_no": idx + 1,
         "ext_a": f"val-{key}-a", "ext_b": f"val-{key}-b", "ext_c": idx,
         "created_at": time.time()}
        for idx, key in enumerate(record_keys)
    ]
    ew._write_jsonl(artifact, records)
    ew.run_action(window["id"], "启动抽取")
    close = ew.close_window(window["id"])
    assert "error" not in close
    return window["id"], window_code


class TestOverlappingWindowBackfill:

    def test_full_overlap_claim_correction_and_trace(self):
        _make_source()
        # 两个相邻交叠窗口：R-SHARED 同时落在 A（前窗）与 B（后窗）
        id_a, code_a = _closed_window("SOUR-BF", "A", ["R-1", "R-SHARED", "R-2"])
        id_b, code_b = _closed_window("SOUR-BF", "B", ["R-SHARED", "R-3"])

        bf = BackfillManifestService()
        # 1) 两个窗口各自筛出缺失记录清单（此时均未发送，全部缺失）
        screen_a = bf.screen_missing(code_a)
        screen_b = bf.screen_missing(code_b)
        assert "error" not in screen_a and "error" not in screen_b
        assert screen_a["missing_count"] == 3
        assert screen_b["missing_count"] == 2
        manifest_a, manifest_b = screen_a["manifest_code"], screen_b["manifest_code"]

        # 清单产物可直接交付传输
        assert Path(screen_a["artifact_path"]).exists()
        assert Path(screen_b["artifact_path"]).exists()

        # 2) 先领取后一窗 B 的清单并实际发送差量分片
        tb = TransferBatchService()
        claim_b = tb.claim_backfill_manifest(manifest_b)
        assert "error" not in claim_b
        batch_b = claim_b["batch_id"]
        send_b = tb.transfer_backfill(batch_b, chunk_size=2)
        assert "error" not in send_b
        assert send_b["records_transferred"] == 2
        assert "R-SHARED" in send_b["sent_keys"]
        # 差量分片产物带 chunk_index，可按分片对照
        assert all("chunk_index" in c for c in send_b["chunks"])

        # 3) 前窗 A 在筛选结果确认后收到 R-SHARED 的更正
        correction = ExtractWindowService().apply_correction(
            id_a, "R-SHARED", {"ext_b": "corrected-by-window-A"})
        assert "error" not in correction
        assert correction["revision"] == 1

        # 4) 再领取前窗 A：交叠记录已被 B 实际发送，本轮不可由 A 重复发送
        claim_a = tb.claim_backfill_manifest(manifest_a)
        assert "error" in claim_a
        conflict_keys = {c["record_key"] for c in claim_a["conflicts"]}
        assert "R-SHARED" in conflict_keys
        # 被拒批次登记为异常失败，未产生发送
        rejected_batch = next(
            r for r in store.rows("transfer_batch") if r["批次编号"] == claim_a["batch_code"])
        assert rejected_batch["status"] == "已失败"

        # 5) 对照分片回查：R-SHARED 实际归属后一窗 B
        trace_shared = tb.trace_backfill_record("R-SHARED")
        assert trace_shared["state"] == "sent"
        owners = {hit["window_code"] for hit in trace_shared["processed_by"]}
        assert owners == {code_b}
        assert all(hit["chunk_index"] is not None for hit in trace_shared["processed_by"])

        # 6) A 清单中尚未发送的待补记录及原因可解释
        report = tb.backfill_unsent_report(manifest_a)
        unsent_keys = {item["record_key"] for item in report["unsent"]}
        assert {"R-1", "R-2", "R-SHARED"} <= unsent_keys
        shared_item = next(i for i in report["unsent"] if i["record_key"] == "R-SHARED")
        assert shared_item["state"] == "sent_by_other_window"
        assert code_b in shared_item["reason"]

        # 7) 重新打开旧清单 B：引用的窗口版本与已发范围仍可回查解释
        reopen = bf.reopen(manifest_b)
        assert "error" not in reopen
        assert reopen["window_version"] == screen_b["window_version"]
        reference = bf.read_manifest_meta(manifest_b)
        assert reference["close_summary"]["window_code"] == code_b
        assert reference["close_summary"]["checksum"]
        assert reference["sent_scope"]["window_code"] == code_b
        # 确认快照是确认时刻固化的，窗口 A 的更正不影响 B 的引用结果
        assert reference["record_keys"] == ["R-SHARED", "R-3"]

        # 8) 已发送清单 B 的每条源记录归属唯一，账本中不存在重复发送
        ledger = bf._read_ledger()
        shared_trail = [e for e in ledger if e["record_key"] == "R-SHARED"]
        windows = {e["window_code"] for e in shared_trail if e["state"] == "sent"}
        assert windows == {code_b}

    def test_screen_requires_closed_window(self):
        _make_source()
        ew = ExtractWindowService()
        window = ew.create_window("SOUR-BF", count=3)
        result = BackfillManifestService().screen_missing(window["窗口编号"])
        assert "error" in result
        assert "闭合" in result["error"]

    def test_manifest_reference_roundtrip(self):
        _make_source()
        _, code = _closed_window("SOUR-BF", "C", ["R-9"])
        bf = BackfillManifestService()
        screen = bf.screen_missing(code)
        meta = bf.read_manifest_meta(screen["manifest_code"])
        # 既有确认清单必须能回查窗口版本与已发出范围
        assert meta["window_version"] == screen["window_version"]
        assert meta["window_status_at_screen"] == "已闭合"
        assert "sent_keys" in meta["sent_scope"]
        assert meta["record_keys"] == ["R-9"]

    def test_trace_unknown_record_explains_unsent(self):
        result = TransferBatchService().trace_backfill_record("R-NEVER-EXIST")
        assert result["state"] == "unsent"
        assert result["reason"]


class TestBackfillRouterBindings:
    """补传专用路由绑定冒烟：直接调用路由函数，验证请求模型与链路贯通。"""

    def test_router_endpoints_chain(self):
        _make_source()
        id_a, code_a = _closed_window("SOUR-BF", "D", ["R-X1", "R-X2"])

        screen = backfill_router.screen_missing(
            backfill_router.ScreenPayload(window_code=code_a))
        assert "manifest_code" in screen
        manifest = screen["manifest_code"]

        reference = backfill_router.manifest_reference(manifest)
        assert reference["window_version"] == screen["window_version"]

        claim = backfill_router.claim_manifest(
            backfill_router.ClaimPayload(manifest_code=manifest))
        assert "batch_id" in claim

        sent = backfill_router.transfer_backfill(
            backfill_router.BatchIdPayload(batch_id=claim["batch_id"], chunk_size=1))
        assert sent["records_transferred"] == 2

        trace = backfill_router.trace_record(
            backfill_router.RecordTracePayload(record_key="R-X1"))
        assert trace["state"] == "sent"

        # 通用资源路由仍可列出补传清单
        page, total = BackfillManifestService().list_entries()
        assert total >= 1
        assert any(item["清单编号"] == manifest for item in page)

        # 更正接口
        correction = backfill_router.apply_correction(backfill_router.CorrectionPayload(
            window_id=id_a, record_key="R-X1", corrected_fields={"ext_b": "fixed"}))
        assert correction.get("revision") == 1

        reopen = backfill_router.reopen_manifest(
            backfill_router.ReopenPayload(manifest_code=manifest))
        assert reopen["status"] == "已重开"

        report = backfill_router.unsent_report(
            backfill_router.ManifestPayload(manifest_code=manifest))
        assert "unsent" in report
