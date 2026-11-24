"""补传清单领域服务：按闭合窗口摘要查询缺失记录，落盘为可交付传输的待补传数据清单。

核心职责
--------
1. ``screen_missing``：对**已闭合**增量窗口，依据窗口闭合摘要（记录条数、校验值）
   与已发送范围（传输产物/断点）比对，筛出仍缺失的源记录，生成版本化的待补传数据
   清单（jsonl 产物）并登记清单条目。
2. 同一条源记录在本轮只允许由一个窗口发送：筛选时校验源记录是否已被其它窗口的清单
   领用或已被实际传输，交叠记录会被排除并记录归属。
3. 清单条目固化所引用的窗口版本（窗口编号、窗口状态、闭合摘要校验值、产物版本号）
   与当时已发出范围，旧清单即使窗口后来收到更正也能回查到其引用结果。
4. 提供归属回查：对交叠源记录给出实际处理窗口/批次，对仍未发送的待补记录给出原因。

数据产物
--------
- ``data/backfill/<清单编号>.jsonl``        清单数据行（待补传记录，含窗口版本与来源）
- ``data/backfill/<清单编号>_meta.json``    清单确认快照（引用窗口版本、已发范围、交叠归属）
- ``data/events/backfill_manifest_log.jsonl``  清单生命周期事件
"""
from __future__ import annotations

import json
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class BackfillManifestService(BaseModuleService):
    MODULE = "backfill_manifest"
    ENTITY = "补传清单"
    OPERATION_TERM = "筛选"
    LIST_FIELDS = [
        "清单编号", "窗口编号", "窗口版本", "缺失条数", "已领条数", "清单状态",
        "引用校验值", "领取批次",
    ]
    REQUIRED_FIELDS = ["清单编号", "窗口编号"]
    # 草稿→已确认（筛选结果落盘即确认）→已领用（被传输批次领取）→已发送 / 已重开
    STATUS_ORDER = ["草稿", "已确认", "已领用", "已发送", "已重开"]
    ACTION_RULES = {
        "确认清单": "已确认",
        "领用清单": "已领用",
        "标记发送": "已发送",
        "重新打开": "已重开",
    }
    NEGATIVE_ACTIONS: list[str] = []

    # ------------------------------------------------------------------
    # 窗口闭合摘要 / 已发送范围读取
    # ------------------------------------------------------------------

    def _window_entry(self, window_code: str) -> dict[str, Any] | None:
        for row in store.rows("extract_window"):
            if row.get("窗口编号") == window_code:
                return row
        return None

    def _close_summary(self, window_code: str) -> dict[str, Any] | None:
        """从闭合事件日志回查指定窗口最近一次闭合摘要（条数、校验值、闭合时间）。"""
        log = self._read_jsonl(self._data_dir("events") / "window_close_log.jsonl")
        matches = [e for e in log if e.get("window_code") == window_code and e.get("action") == "close"]
        return matches[-1] if matches else None

    def _extracted_records(self, window_code: str) -> list[dict[str, Any]]:
        """读取窗口抽取产物，作为该窗口版本下的源记录集合。"""
        return self._read_jsonl(self._data_dir("extracted") / f"{window_code}.jsonl")

    @staticmethod
    def _record_key(record: dict[str, Any]) -> str:
        """源记录唯一标识：优先用业务主键，否则回退到窗口内行号。"""
        return str(record.get("record_key") or record.get("source_record_id")
                   or f"{record.get('batch_key', '')}#L{record.get('line_no')}")

    def _sent_scope(self, window_code: str) -> dict[str, Any]:
        """汇总该窗口关联批次已经实际发出的源记录范围与批次清单。"""
        sent_keys: set[str] = set()
        batches: list[dict[str, Any]] = []
        for batch in store.rows("transfer_batch"):
            if batch.get("窗口编号") != window_code:
                continue
            batch_code = batch.get("批次编号", "")
            batch_records = self._read_jsonl(self._data_dir("batches") / f"{batch_code}.jsonl")
            keys = [self._record_key(r) for r in batch_records]
            sent_keys.update(keys)
            batches.append({
                "batch_code": batch_code,
                "status": batch.get("status"),
                "record_count": len(batch_records),
                "record_keys": keys,
            })
        return {"window_code": window_code, "sent_keys": sorted(sent_keys), "batches": batches}

    # ------------------------------------------------------------------
    # 全局领用地账本：同一源记录本轮只可由一个窗口发送
    # ------------------------------------------------------------------

    def _ledger_file(self):
        return self._data_dir("backfill") / "claim_ledger.jsonl"

    def _read_ledger(self) -> list[dict[str, Any]]:
        """读取源记录领用地账本（含已领用与已发送两类占用）。"""
        return self._read_jsonl(self._ledger_file())

    def _append_ledger(self, entries: list[dict[str, Any]]) -> None:
        self._write_jsonl(self._ledger_file(), entries)

    def _claimed_by_other(self, record_key: str, window_code: str) -> dict[str, Any] | None:
        """若源记录已被其它窗口领用/发送，返回占用账本条目，否则 None。"""
        for item in self._read_ledger():
            if item.get("record_key") != record_key:
                continue
            if item.get("window_code") == window_code:
                return None
            return item
        return None

    # ------------------------------------------------------------------
    # 核心能力：按闭合窗口摘要筛选缺失记录，落盘待补传数据清单
    # ------------------------------------------------------------------

    def screen_missing(self, window_code: str, extra_missing: list[str] | None = None) -> dict[str, Any]:
        """按闭合窗口摘要筛选缺失源记录，保存为待补传数据清单。

        判定一条源记录「缺失」的依据：
        1. 窗口必须已闭合，且能回查到闭合摘要；
        2. 该记录不在本窗口关联批次的已发送范围内（或被显式列入 extra_missing）；
        3. 同一源记录未被其它窗口的清单领用或实际发送（交叠独占校验）。

        产物中同时保留被排除的交叠记录及其归属窗口，供传输侧对照。
        """
        window = self._window_entry(window_code)
        if window is None:
            return {"error": f"抽取窗口 {window_code} 不存在"}
        if window.get("status") != "已闭合":
            return {"error": f"窗口 {window_code} 尚未闭合（当前状态：{window.get('status')}），不能按闭合摘要筛选缺失记录"}

        summary = self._close_summary(window_code)
        if summary is None:
            return {"error": f"窗口 {window_code} 缺少闭合摘要，无法确认窗口版本"}

        records = self._extracted_records(window_code)
        if not records:
            return {"error": f"窗口 {window_code} 没有可读的抽取产物，无法筛选缺失记录"}

        # 窗口版本：闭合摘要校验值 + 产物当前校验值共同固化
        artifact_checksum = self._compute_checksum(str(records))
        window_version = f"v{summary.get('checksum', 'na')[:8]}-{artifact_checksum[:8]}"
        sent_scope = self._sent_scope(window_code)
        sent_keys = set(sent_scope["sent_keys"])
        extra = set(extra_missing or [])

        manifest_code = f"BACK-{window_code}-{int(time.time())}"
        existing = {r.get("清单编号") for r in store.rows(self.MODULE)}
        unique_code, suffix = manifest_code, 2
        while unique_code in existing:
            unique_code = f"{manifest_code}-{suffix}"
            suffix += 1
        manifest_code = unique_code
        missing_rows: list[dict[str, Any]] = []
        overlaps: list[dict[str, Any]] = []
        for rec in records:
            key = self._record_key(rec)
            holder = self._claimed_by_other(key, window_code)
            if holder is not None:
                overlaps.append({
                    "record_key": key,
                    "owner_window": holder.get("window_code"),
                    "owner_manifest": holder.get("manifest_code"),
                    "owner_batch": holder.get("batch_code"),
                    "claim_state": holder.get("state"),
                    "reason": "交叠记录已由相邻窗口领用/发送",
                })
                continue
            if key in sent_keys and key not in extra:
                continue
            missing_rows.append({
                "record_key": key,
                "source_window": window_code,
                "window_version": window_version,
                "payload": rec,
                "missing_reason": "显式标记缺失" if key in extra else "窗口闭合摘要比对：未在已发送范围内",
                "screened_at": time.time(),
            })

        # 登记清单条目（store），固化引用的窗口版本与已发范围
        values = {
            "清单编号": manifest_code,
            "窗口编号": window_code,
            "窗口版本": window_version,
            "缺失条数": str(len(missing_rows)),
            "已领条数": "0",
            "清单状态": "已确认",
            "引用校验值": summary.get("checksum", ""),
            "领取批次": "",
        }
        entry, missing_fields = self.create_entry(values)
        if entry is None:
            return {"error": f"创建补传清单失败：缺少字段 {missing_fields}"}
        entry["status"] = "已确认"
        entry["pending"] = False

        # 清单数据产物：可直接交给传输的待补传数据清单
        artifact = self._data_dir("backfill") / f"{manifest_code}.jsonl"
        self._write_jsonl(artifact, missing_rows)

        # 清单确认快照：旧清单日后重新打开时，凭此回查引用的窗口版本与已发范围
        meta = {
            "manifest_code": manifest_code,
            "manifest_id": entry["id"],
            "window_code": window_code,
            "window_id": window.get("id"),
            "window_status_at_screen": window.get("status"),
            "window_version": window_version,
            "close_summary": summary,
            "artifact_checksum": artifact_checksum,
            "sent_scope": sent_scope,
            "missing_count": len(missing_rows),
            "overlap_count": len(overlaps),
            "overlaps": overlaps,
            "record_keys": [row["record_key"] for row in missing_rows],
            "confirmed_at": time.time(),
        }
        meta_file = self._data_dir("backfill") / f"{manifest_code}_meta.json"
        with open(meta_file, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2, default=str)

        self._write_jsonl(
            self._data_dir("events") / "backfill_manifest_log.jsonl",
            [{"action": "screen", "manifest_code": manifest_code, "window_code": window_code,
              "window_version": window_version, "missing_count": len(missing_rows),
              "overlap_count": len(overlaps), "at": time.time()}],
        )
        return {
            "manifest_code": manifest_code,
            "manifest_id": entry["id"],
            "window_code": window_code,
            "window_version": window_version,
            "missing_count": len(missing_rows),
            "overlap_count": len(overlaps),
            "overlaps": overlaps,
            "artifact_path": str(artifact),
            "meta_path": str(meta_file),
        }

    # ------------------------------------------------------------------
    # 领用 / 发送状态推进（由传输批次服务回调）
    # ------------------------------------------------------------------

    def find_manifest(self, manifest_code: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("清单编号") == manifest_code:
                return row
        return None

    def read_manifest_meta(self, manifest_code: str) -> dict[str, Any] | None:
        """读取清单确认快照；文件缺失返回 None（旧清单引用结果不可解释时的判据）。"""
        meta_file = self._data_dir("backfill") / f"{manifest_code}_meta.json"
        if not meta_file.exists():
            return None
        with open(meta_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def claim(self, manifest_code: str, batch_code: str) -> dict[str, Any]:
        """传输批次领用清单：状态置已领用，并把源记录占用写入领用地账本。"""
        entry = self.find_manifest(manifest_code)
        if entry is None:
            return {"error": f"补传清单 {manifest_code} 不存在"}
        if entry.get("status") not in ("已确认", "已重开"):
            return {"error": f"清单 {manifest_code} 当前状态 {entry.get('status')}，不可领用"}
        meta = self.read_manifest_meta(manifest_code)
        if meta is None:
            return {"error": f"清单 {manifest_code} 的确认快照缺失，引用结果不可解释，拒绝领用"}

        # 领用前再次做交叠独占校验，防止两个批次并发领取相邻窗口清单
        conflicts: list[dict[str, Any]] = []
        for key in meta["record_keys"]:
            holder = self._claimed_by_other(key, meta["window_code"])
            if holder is not None:
                conflicts.append({"record_key": key, "holder": holder})
        if conflicts:
            return {"error": "存在已被其它窗口占用的交叠记录，领用被拒绝", "conflicts": conflicts}

        entry["status"] = "已领用"
        entry["pending"] = True
        entry["领取批次"] = batch_code
        entry["已领条数"] = str(len(meta["record_keys"]))
        self._append_ledger([
            {"record_key": key, "window_code": meta["window_code"], "manifest_code": manifest_code,
             "batch_code": batch_code, "state": "claimed", "at": time.time()}
            for key in meta["record_keys"]
        ])
        self._write_jsonl(
            self._data_dir("events") / "backfill_manifest_log.jsonl",
            [{"action": "claim", "manifest_code": manifest_code, "batch_code": batch_code,
              "record_count": len(meta["record_keys"]), "at": time.time()}],
        )
        return {"manifest_code": manifest_code, "batch_code": batch_code,
                "claimed": len(meta["record_keys"]), "status": "已领用"}

    def mark_sent(self, manifest_code: str, batch_code: str, sent_keys: list[str]) -> dict[str, Any]:
        """实际发送完成后把账本占用升级为 sent，并推进清单状态。"""
        entry = self.find_manifest(manifest_code)
        if entry is None:
            return {"error": f"补传清单 {manifest_code} 不存在"}
        entry["status"] = "已发送"
        entry["pending"] = False
        self._append_ledger([
            {"record_key": key, "window_code": entry.get("窗口编号"), "manifest_code": manifest_code,
             "batch_code": batch_code, "state": "sent", "at": time.time()}
            for key in sent_keys
        ])
        self._write_jsonl(
            self._data_dir("events") / "backfill_manifest_log.jsonl",
            [{"action": "sent", "manifest_code": manifest_code, "batch_code": batch_code,
              "sent_count": len(sent_keys), "at": time.time()}],
        )
        return {"manifest_code": manifest_code, "status": "已发送", "sent": len(sent_keys)}

    def reopen(self, manifest_code: str) -> dict[str, Any]:
        """重新打开旧清单：状态置已重开，但确认快照保持不变，引用结果仍可回查解释。"""
        entry = self.find_manifest(manifest_code)
        if entry is None:
            return {"error": f"补传清单 {manifest_code} 不存在"}
        meta = self.read_manifest_meta(manifest_code)
        if meta is None:
            return {"error": f"清单 {manifest_code} 确认快照缺失，无法解释其引用结果"}
        entry["status"] = "已重开"
        entry["pending"] = True
        self._write_jsonl(
            self._data_dir("events") / "backfill_manifest_log.jsonl",
            [{"action": "reopen", "manifest_code": manifest_code,
              "window_version": meta.get("window_version"), "at": time.time()}],
        )
        return {"manifest_code": manifest_code, "status": "已重开",
                "window_version": meta.get("window_version"),
                "close_summary": meta.get("close_summary"),
                "sent_scope": meta.get("sent_scope")}

    # ------------------------------------------------------------------
    # 归属回查：交叠记录被谁处理、待补记录为何未发送
    # ------------------------------------------------------------------

    def trace_record(self, record_key: str) -> dict[str, Any]:
        """对照分片与账本回查单条源记录的实际处理归属/未发送原因。"""
        ledger = [e for e in self._read_ledger() if e.get("record_key") == record_key]
        # 在所有传输产物中定位实际分片
        chunk_hits: list[dict[str, Any]] = []
        for batch in store.rows("transfer_batch"):
            batch_code = batch.get("批次编号", "")
            for rec in self._read_jsonl(self._data_dir("batches") / f"{batch_code}.jsonl"):
                if self._record_key(rec) == record_key:
                    chunk_hits.append({
                        "batch_code": batch_code,
                        "window_code": batch.get("窗口编号"),
                        "chunk_index": rec.get("chunk_index"),
                        "transferred_at": rec.get("transferred_at"),
                    })
        result: dict[str, Any] = {"record_key": record_key}
        if chunk_hits:
            result["state"] = "sent"
            result["processed_by"] = chunk_hits
        elif ledger:
            latest = ledger[-1]
            result["state"] = latest.get("state")
            result["owner_window"] = latest.get("window_code")
            result["owner_manifest"] = latest.get("manifest_code")
            result["owner_batch"] = latest.get("batch_code")
            result["reason"] = "已领用但尚未在传输产物中出现，待差量分片发送"
        else:
            result["state"] = "unsent"
            result["reason"] = "未进入任何补传清单：未被筛选为缺失或筛选时被交叠独占规则排除"
        result["ledger_trail"] = ledger
        return result

    def pending_unsent_report(self, manifest_code: str) -> dict[str, Any]:
        """回查清单内未由本清单发送的待补记录及原因。

        包含两类：仍未发送（未领用/未出现在传输产物中）与交叠后被其它窗口
        实际发送的记录——后者在本清单视角下同样是「未由本清单发送」，
        报告中给出实际处理窗口作为原因。
        """
        meta = self.read_manifest_meta(manifest_code)
        if meta is None:
            return {"error": f"清单 {manifest_code} 确认快照缺失"}
        own_window = meta["window_code"]
        unsent: list[dict[str, Any]] = []
        for key in meta["record_keys"]:
            trace = self.trace_record(key)
            if trace.get("state") == "sent":
                owners = {hit.get("window_code") for hit in trace.get("processed_by", [])}
                if owners == {own_window}:
                    continue  # 已由本清单窗口发送，不属于未发送项
                unsent.append({
                    "record_key": key, "state": "sent_by_other_window",
                    "reason": f"交叠记录已由其它窗口实际发送：{sorted(o for o in owners if o)}",
                    "processed_by": trace.get("processed_by"),
                })
            else:
                unsent.append({"record_key": key, "state": trace.get("state"),
                               "reason": trace.get("reason"),
                               "owner_manifest": trace.get("owner_manifest")})
        return {"manifest_code": manifest_code, "total": len(meta["record_keys"]),
                "unsent_count": len(unsent), "unsent": unsent}
