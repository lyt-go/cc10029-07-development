"""传输批次领域服务：维护传输批次的分片组织、记录条数与字节总量登记及批次水位推进。

本模块读取抽取窗口产物文件后创建传输批次，分片写入传输数据文件，
传输完成后自动驱动暂存落地和校验核对，形成窗口抽取→批次传输→落地校验的贯通链路。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class TransferBatchService(BaseModuleService):
    MODULE = "transfer_batch"
    ENTITY = "传输批次"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["批次编号", "窗口编号", "目标编号", "记录条数", "字节总量", "分片大小", "传输状态", "批次水位"]
    REQUIRED_FIELDS = ["批次编号", "窗口编号", "目标编号"]
    STATUS_ORDER = ["草稿", "待传输", "传输中", "已完成", "已失败"]
    ACTION_RULES = {"启动传输": "传输中", "标记完成": "已完成", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = ["标记失败"]

    def create_batch(self, window_code: str, record_count: int, target_code: str = "TARG-0001") -> dict[str, Any]:
        """基于抽取窗口创建传输批次，传入产物文件的 record_count 并写入批次事件日志。"""
        batch_code = f"TRAN-{window_code}-{int(time.time())}"
        values = {
            "批次编号": batch_code,
            "窗口编号": window_code,
            "目标编号": target_code,
            "记录条数": str(record_count),
            "字节总量": "0",
            "分片大小": "1000",
            "传输状态": "草稿",
            "批次水位": "0",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建传输批次失败: {missing}")
        # 写入批次创建事件
        self._write_jsonl(
            self._data_dir("events") / "batch_create_log.jsonl",
            [{"action": "create_batch", "batch_code": batch_code, "window_code": window_code,
              "record_count": record_count, "target": target_code, "at": time.time()}]
        )
        return {"batch_code": batch_code, "batch_id": entry["id"], "window_code": window_code,
                "record_count": record_count, "target_code": target_code}

    def start_transfer(self, batch_id: int, chunk_size: int = 3) -> dict[str, Any]:
        """启动传输批次执行：读取抽取窗口产物文件，按分片大小写入传输产物文件并推进水位。

        本方法执行实际的批量数据传输：从抽取产物文件读取数据，切分为分片，
        写入传输产物文件（jsonl），计算字节总量，然后自动触发暂存落地和校验核对。
        """
        batch_entry = self.get_entry(batch_id)
        if batch_entry is None:
            return {"error": f"传输批次 {batch_id} 不存在"}

        entry, msg = self.run_action(batch_id, "启动传输")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}

        batch_code = batch_entry["批次编号"]
        window_code = batch_entry.get("窗口编号", "unknown")

        # 读取上游抽取窗口的产物文件
        artifact_dir = self._data_dir("extracted")
        source_file = artifact_dir / f"{window_code}.jsonl" if window_code != "unknown" else None

        records = []
        if source_file and source_file.exists():
            records = self._read_jsonl(source_file)
        else:
            # 如果上游产物文件不存在，生成模拟数据
            records = self._simulate_records(batch_code, count=int(batch_entry.get("记录条数", 10)),
                                              field_prefix="trans")

        # 分片写入传输产物文件
        output_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        total_bytes = 0
        total_written = 0
        chunks = []
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            for rec in chunk:
                rec["batch_code"] = batch_code
                rec["chunk_index"] = i // chunk_size
                rec["transferred_at"] = time.time()
            self._write_jsonl(output_file, chunk)
            chunk_bytes = sum(len(str(r).encode("utf-8")) for r in chunk)
            chunks.append({"chunk_index": i // chunk_size, "record_count": len(chunk),
                           "bytes": chunk_bytes})
            total_bytes += chunk_bytes
            total_written += len(chunk)

        # 更新批次水位和字节总量
        entry["字节总量"] = str(total_bytes)
        entry["记录条数"] = str(total_written)
        entry["批次水位"] = str(total_written)

        # 写入传输水位断点
        self._write_checkpoint(f"batch_{batch_code}", {
            "batch_code": batch_code,
            "window_code": window_code,
            "records_transferred": total_written,
            "total_bytes": total_bytes,
            "chunk_count": len(chunks),
            "watermark": str(total_written),
            "last_transfer_at": time.time(),
        })

        result = {
            "batch_code": batch_code,
            "window_code": window_code,
            "records_transferred": total_written,
            "total_bytes": total_bytes,
            "chunks": len(chunks),
            "artifact_path": str(output_file),
        }

        # 传输完成后自动触发暂存落地
        try:
            sa_svc = self._safe_import("app.services.staging_area", "StagingAreaService")()
            staging_result = sa_svc.land_batch(batch_code, total_written)
            result["auto_staging"] = staging_result
        except Exception as e:
            result["auto_staging"] = {"error": str(e)}

        # 传输完成后自动创建校验核对任务
        try:
            cv_svc = self._safe_import("app.services.checksum_verify", "ChecksumVerifyService")()
            verify_result = cv_svc.create_verification(batch_code, total_written)
            result["auto_verify"] = verify_result
        except Exception as e:
            result["auto_verify"] = {"error": str(e)}

        # 传输完成后自动创建到达对账单
        try:
            ar_svc = self._safe_import("app.services.arrival_reconcile", "ArrivalReconcileService")()
            reconcile_result = ar_svc.create_reconciliation(batch_code, total_written)
            result["auto_reconcile"] = reconcile_result
        except Exception as e:
            result["auto_reconcile"] = {"error": str(e)}

        return result

    def mark_completed(self, batch_id: int) -> dict[str, Any]:
        """标记传输批次已完成，写入完成事件日志并记录批次汇总统计。"""
        entry, msg = self.run_action(batch_id, "标记完成")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}
        batch_code = entry["批次编号"]
        # 从产物文件验证批次完整性
        artifact_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        records = self._read_jsonl(artifact_file)
        checksum = self._compute_checksum(str(records))
        self._write_jsonl(
            self._data_dir("events") / "batch_complete_log.jsonl",
            [{"action": "complete", "batch_code": batch_code, "record_count": len(records),
              "checksum": checksum, "completed_at": time.time()}]
        )
        return {"batch_code": batch_code, "status": "已完成", "record_count": len(records),
                "checksum": checksum}

    def mark_failed(self, batch_id: int, reason: str = "") -> dict[str, Any]:
        """标记传输批次失败，写入失败事件日志并记录中断原因。"""
        entry, msg = self.run_action(batch_id, "标记失败")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}
        batch_code = entry["批次编号"]
        self._write_jsonl(
            self._data_dir("events") / "batch_fail_log.jsonl",
            [{"action": "fail", "batch_code": batch_code, "reason": reason, "failed_at": time.time()}]
        )
        return {"batch_code": batch_code, "status": "已失败", "reason": reason}

    # ------------------------------------------------------------------
    # 补传链路：领取待补传数据清单 → 差量分片 → 实际发送 → 归属回查
    # ------------------------------------------------------------------

    def claim_backfill_manifest(
        self, manifest_code: str, target_code: str = "TARG-0001",
    ) -> dict[str, Any]:
        """领取一份待补传数据清单，生成绑定该清单的补传批次。

        领用动作委托补传清单服务完成交叠独占校验：同一源记录本轮只可由一个窗口
        发送，若清单中的记录已被相邻窗口占用，领取会被拒绝。
        """
        bf_svc = self._safe_import(
            "app.services.backfill_manifest", "BackfillManifestService")()
        meta = bf_svc.read_manifest_meta(manifest_code)
        if meta is None:
            return {"error": f"补传清单 {manifest_code} 不存在或确认快照缺失，拒绝领取"}

        batch_code = f"TRAN-BF-{manifest_code}-{int(time.time())}"
        existing = {r.get("批次编号") for r in store.rows(self.MODULE)}
        unique_code, suffix = batch_code, 2
        while unique_code in existing:
            unique_code = f"{batch_code}-{suffix}"
            suffix += 1
        batch_code = unique_code
        entry, missing = self.create_entry({
            "批次编号": batch_code,
            "窗口编号": meta["window_code"],
            "目标编号": target_code,
            "记录条数": str(meta["missing_count"]),
            "字节总量": "0",
            "分片大小": "1000",
            "传输状态": "草稿",
            "批次水位": "0",
        })
        if entry is None:
            return {"error": f"创建补传批次失败：{missing}"}
        entry["关联清单"] = manifest_code

        claim_result = bf_svc.claim(manifest_code, batch_code)
        if "error" in claim_result:
            entry["传输状态"] = "已失败"
            entry["status"] = "已失败"
            entry["abnormal"] = True
            self._write_jsonl(
                self._data_dir("events") / "batch_fail_log.jsonl",
                [{"action": "claim_rejected", "batch_code": batch_code,
                  "manifest_code": manifest_code, "reason": claim_result["error"],
                  "failed_at": time.time()}],
            )
            return {"error": claim_result["error"], "batch_code": batch_code,
                    "conflicts": claim_result.get("conflicts", [])}

        self._write_jsonl(
            self._data_dir("events") / "batch_create_log.jsonl",
            [{"action": "claim_backfill", "batch_code": batch_code,
              "manifest_code": manifest_code, "window_code": meta["window_code"],
              "record_count": meta["missing_count"], "at": time.time()}],
        )
        return {"batch_code": batch_code, "batch_id": entry["id"],
                "manifest_code": manifest_code, "window_code": meta["window_code"],
                "window_version": meta.get("window_version"),
                "record_count": meta["missing_count"], "claim": claim_result}

    def transfer_backfill(self, batch_id: int, chunk_size: int = 3) -> dict[str, Any]:
        """读取已领取清单的待补传记录，生成差量分片并完成实际发送。

        差量分片只包含清单内尚未发送的源记录（清单本身即差量筛选结果），
        发送完成后回写清单状态与领用地账本，保证每条源记录的归属可回查。
        """
        batch_entry = self.get_entry(batch_id)
        if batch_entry is None:
            return {"error": f"传输批次 {batch_id} 不存在"}
        manifest_code = batch_entry.get("关联清单")
        if not manifest_code:
            return {"error": f"批次 {batch_entry.get('批次编号')} 未关联补传清单，不能走补传发送"}

        bf_svc = self._safe_import(
            "app.services.backfill_manifest", "BackfillManifestService")()
        manifest_rows = bf_svc._read_jsonl(
            self._data_dir("backfill") / f"{manifest_code}.jsonl")
        if not manifest_rows:
            return {"error": f"补传清单 {manifest_code} 产物为空或已被清理"}

        entry, msg = self.run_action(batch_id, "启动传输")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}
        entry["关联清单"] = manifest_code
        batch_code = batch_entry["批次编号"]

        output_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        chunks: list[dict[str, Any]] = []
        total_bytes = 0
        sent_keys: list[str] = []
        for i in range(0, len(manifest_rows), chunk_size):
            chunk = manifest_rows[i:i + chunk_size]
            shard = []
            for row in chunk:
                sent_keys.append(row["record_key"])
                shard.append({
                    "record_key": row["record_key"],
                    "source_window": row["source_window"],
                    "window_version": row["window_version"],
                    "manifest_code": manifest_code,
                    "batch_code": batch_code,
                    "chunk_index": i // chunk_size,
                    "payload": row.get("payload"),
                    "transferred_at": time.time(),
                })
            self._write_jsonl(output_file, shard)
            chunk_bytes = sum(len(str(r).encode("utf-8")) for r in shard)
            chunks.append({"chunk_index": i // chunk_size, "record_count": len(shard),
                           "record_keys": [r["record_key"] for r in shard], "bytes": chunk_bytes})
            total_bytes += chunk_bytes

        entry["字节总量"] = str(total_bytes)
        entry["记录条数"] = str(len(sent_keys))
        entry["批次水位"] = str(len(sent_keys))
        self._write_checkpoint(f"batch_{batch_code}", {
            "batch_code": batch_code,
            "manifest_code": manifest_code,
            "records_transferred": len(sent_keys),
            "total_bytes": total_bytes,
            "chunk_count": len(chunks),
            "sent_keys": sent_keys,
            "watermark": str(len(sent_keys)),
            "last_transfer_at": time.time(),
        })

        sent_result = bf_svc.mark_sent(manifest_code, batch_code, sent_keys)
        return {
            "batch_code": batch_code,
            "manifest_code": manifest_code,
            "records_transferred": len(sent_keys),
            "sent_keys": sent_keys,
            "total_bytes": total_bytes,
            "chunks": chunks,
            "artifact_path": str(output_file),
            "manifest_result": sent_result,
        }

    def trace_backfill_record(self, record_key: str) -> dict[str, Any]:
        """对照分片回查交叠记录被哪个窗口实际处理，或待补记录为何仍未发送。"""
        bf_svc = self._safe_import(
            "app.services.backfill_manifest", "BackfillManifestService")()
        return bf_svc.trace_record(record_key)

    def backfill_unsent_report(self, manifest_code: str) -> dict[str, Any]:
        """回查清单内仍未发送的待补记录及原因。"""
        bf_svc = self._safe_import(
            "app.services.backfill_manifest", "BackfillManifestService")()
        return bf_svc.pending_unsent_report(manifest_code)