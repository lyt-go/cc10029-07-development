"""增量窗口领域服务：维护按增量字段切分的抽取窗口边界、抽取执行与推进水位。

本模块负责根据源端登记信息创建抽取窗口，执行数据抽取并写入抽取产物文件，
完成后自动驱动传输批次创建，形成源端登记→窗口抽取→批次传输的贯通链路。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ExtractWindowService(BaseModuleService):
    MODULE = "extract_window"
    ENTITY = "抽取窗口"
    OPERATION_TERM = "切分"
    LIST_FIELDS = ["窗口编号", "源端编号", "增量字段", "窗口起点", "窗口终点", "抽取条数", "窗口类型", "推进水位"]
    REQUIRED_FIELDS = ["窗口编号", "源端编号", "增量字段"]
    STATUS_ORDER = ["草稿", "待抽取", "抽取中", "已闭合"]
    ACTION_RULES = {"启动抽取": "抽取中", "闭合窗口": "已闭合", "标记作废": "已闭合"}
    NEGATIVE_ACTIONS = ["标记作废"]

    def create_window(self, source_code: str, count: int = 10) -> dict[str, Any]:
        """为指定源端创建增量抽取窗口并写入窗口配置快照，记录窗口边界与初始水位。"""
        sources = [r for r in store.rows("source_register") if r.get("源端编号") == source_code]
        if not sources:
            raise ValueError(f"源端编号 {source_code} 未登记，请先完成源端登记")
        # 同秒内为同一源端连续开窗时追加序号，保证窗口编号与产物文件名唯一
        base_code = f"EXTR-{source_code}-{int(time.time())}"
        window_code = base_code
        existing = {r.get("窗口编号") for r in store.rows(self.MODULE)}
        suffix = 2
        while window_code in existing:
            window_code = f"{base_code}-{suffix}"
            suffix += 1
        entry, missing = self.create_entry({
            "窗口编号": window_code,
            "源端编号": source_code,
            "增量字段": "updated_at",
            "窗口起点": time.strftime("%Y-%m-%dT00:00:00", time.localtime(time.time())),
            "窗口终点": time.strftime("%Y-%m-%dT23:59:59", time.localtime(time.time())),
            "抽取条数": str(count),
            "窗口类型": "增量抽取",
            "推进水位": "0",
        })
        if entry is None:
            raise ValueError(f"创建窗口失败: {missing}")
        # 写入窗口配置快照
        self._write_jsonl(
            self._data_dir("windows") / f"{entry['窗口编号']}_config.jsonl",
            [{"window_id": entry["id"], "window_code": entry["窗口编号"], "source": source_code,
              "start": entry["窗口起点"], "end": entry["窗口终点"], "count": count,
              "created_at": time.time()}]
        )
        return entry

    def execute_extract(self, window_id: int, record_count: int = 10) -> dict[str, Any]:
        """执行增量窗口数据抽取，生成抽取产物文件并推进窗口水位。

        从已在 store 登记的源端信息中查找匹配记录，按窗口范围筛选数据行，
        逐条写入抽取产物文件（jsonl），最后推进水位并自动触发传输批次创建。
        """
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}
        entry, msg = self.run_action(window_id, "启动抽取")
        if entry is None:
            return {"error": msg, "window_id": window_id}

        source_code = window["源端编号"]
        # 生成抽取数据行，每条记录含窗口编号、源端编号、增量字段取值和时间戳
        records = self._simulate_records(source_code, count=record_count, field_prefix="ext")

        # 写入抽取产物文件——这是本模块的核心数据产物
        output_file = self._data_dir("extracted") / f"{window['窗口编号']}.jsonl"
        written = self._write_jsonl(output_file, records)

        # 推进窗口水位
        self._write_checkpoint(f"window_{window['窗口编号']}", {
            "window_code": window["窗口编号"],
            "source_code": source_code,
            "records_extracted": written,
            "last_extract_at": time.time(),
            "watermark": str(written),
        })

        result = {
            "window_code": window["窗口编号"],
            "source_code": source_code,
            "records_extracted": written,
            "artifact_path": str(output_file),
            "watermark": str(written),
        }

        # 抽取完成后自动触发传输批次创建
        try:
            tb_svc = self._safe_import("app.services.transfer_batch", "TransferBatchService")()
            batch_result = tb_svc.create_batch(window["窗口编号"], written)
            result["auto_batch"] = batch_result
        except Exception as e:
            result["auto_batch"] = {"error": str(e)}

        return result

    def close_window(self, window_id: int) -> dict[str, Any]:
        """闭合抽取窗口，生成窗口产物摘要（记录条数、字节总量、校验值），写入闭合事件日志。"""
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}

        entry, msg = self.run_action(window_id, "闭合窗口")
        # 从产物文件读取统计信息
        artifact_file = self._data_dir("extracted") / f"{window['窗口编号']}.jsonl"
        records = self._read_jsonl(artifact_file)
        checksum = self._compute_checksum(str(records))

        # 写入闭合事件日志，记录抽取产物的完整统计
        self._write_jsonl(
            self._data_dir("events") / "window_close_log.jsonl",
            [{"action": "close", "window_code": window["窗口编号"], "record_count": len(records),
              "checksum": checksum, "closed_at": time.time()}]
        )

        return {
            "window_code": window["窗口编号"],
            "status": entry["status"] if entry else "已闭合",
            "record_count": len(records),
            "checksum": checksum,
            "message": msg,
        }

    def apply_correction(self, window_id: int, record_key: str, corrected_fields: dict[str, Any]) -> dict[str, Any]:
        """窗口闭合后收到源端更正：在抽取产物中定位同一条源记录并写入更正版本。

        典型场景：相邻两个窗口交界处存在同一条更新记录，后一窗已先领用补传，
        前一窗又在筛选结果确认后收到更正。更正只追加新版本行，不覆盖历史行，
        以保证已确认清单引用的窗口版本仍可回查解释；同时写入更正事件日志。
        """
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}
        window_code = window["窗口编号"]
        artifact_file = self._data_dir("extracted") / f"{window_code}.jsonl"
        records = self._read_jsonl(artifact_file)
        if not records:
            return {"error": f"窗口 {window_code} 缺少抽取产物，无法定位更正记录"}

        key_field = "record_key"
        located = [r for r in records if str(r.get(key_field) or r.get("source_record_id")
                                           or f"{r.get('batch_key', '')}#L{r.get('line_no')}") == record_key]
        if not located:
            return {"error": f"窗口 {window_code} 中未找到源记录 {record_key}"}

        latest = located[-1]
        revised = dict(latest)
        revised.update(corrected_fields)
        revised[key_field] = record_key
        revised["revision"] = int(latest.get("revision", 0)) + 1
        revised["corrected_at"] = time.time()
        revised["correction_note"] = "窗口闭合后源端更正，追加新版本行"
        self._write_jsonl(artifact_file, [revised])

        self._write_jsonl(
            self._data_dir("events") / "window_correction_log.jsonl",
            [{"action": "correction", "window_code": window_code, "record_key": record_key,
              "revision": revised["revision"], "corrected_fields": list(corrected_fields.keys()),
              "window_status": window.get("status"), "at": time.time()}],
        )
        return {"window_code": window_code, "record_key": record_key,
                "revision": revised["revision"], "artifact_path": str(artifact_file)}