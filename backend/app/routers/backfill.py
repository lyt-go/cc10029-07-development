"""补传链路专用路由：窗口缺失筛选、清单回查、批次领取/差量发送/归属对照。

通用资源路由（/api/{module}）仍承担补传清单的列表与详情；本路由只承载
无法用通用 CRUD 表达的领域动作，并保持「路由层不做业务判断」的约定。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.backfill_manifest import BackfillManifestService
from app.services.extract_window import ExtractWindowService
from app.services.transfer_batch import TransferBatchService

router = APIRouter(prefix="/api/backfill", tags=["补传链路"])


class ScreenPayload(BaseModel):
    window_code: str
    extra_missing: list[str] = Field(default_factory=list)


class CorrectionPayload(BaseModel):
    window_id: int
    record_key: str
    corrected_fields: dict[str, Any] = Field(default_factory=dict)


class ClaimPayload(BaseModel):
    manifest_code: str
    target_code: str = "TARG-0001"


class BatchIdPayload(BaseModel):
    batch_id: int
    chunk_size: int = 3


class ManifestPayload(BaseModel):
    manifest_code: str


class RecordTracePayload(BaseModel):
    record_key: str


class ReopenPayload(BaseModel):
    manifest_code: str


@router.post("/windows/screen-missing")
def screen_missing(payload: ScreenPayload) -> dict[str, Any]:
    """按闭合窗口摘要查询缺失记录，落盘为待补传数据清单。"""
    return BackfillManifestService().screen_missing(payload.window_code, payload.extra_missing)


@router.post("/windows/correction")
def apply_correction(payload: CorrectionPayload) -> dict[str, Any]:
    """窗口闭合且清单确认后，补入源端更正（追加版本行，不覆盖历史）。"""
    return ExtractWindowService().apply_correction(
        payload.window_id, payload.record_key, payload.corrected_fields)


@router.get("/manifests/{manifest_code}/reference")
def manifest_reference(manifest_code: str) -> dict[str, Any]:
    """回查已确认清单引用的窗口版本与当时已发出范围。"""
    bf = BackfillManifestService()
    meta = bf.read_manifest_meta(manifest_code)
    if meta is None:
        return {"error": f"清单 {manifest_code} 确认快照缺失，引用结果不可解释"}
    return meta


@router.post("/manifests/reopen")
def reopen_manifest(payload: ReopenPayload) -> dict[str, Any]:
    """重新打开旧清单，并回显其引用窗口版本与已发范围，确认仍可解释。"""
    return BackfillManifestService().reopen(payload.manifest_code)


@router.post("/batches/claim")
def claim_manifest(payload: ClaimPayload) -> dict[str, Any]:
    """传输批次领取待补传数据清单（交叠独占校验不通过则拒绝）。"""
    return TransferBatchService().claim_backfill_manifest(
        payload.manifest_code, payload.target_code)


@router.post("/batches/transfer")
def transfer_backfill(payload: BatchIdPayload) -> dict[str, Any]:
    """批次按清单生成差量分片并实际发送。"""
    return TransferBatchService().transfer_backfill(payload.batch_id, payload.chunk_size)


@router.post("/records/trace")
def trace_record(payload: RecordTracePayload) -> dict[str, Any]:
    """对照分片回查交叠记录实际处理窗口，或待补记录未发送原因。"""
    return TransferBatchService().trace_backfill_record(payload.record_key)


@router.post("/manifests/unsent-report")
def unsent_report(payload: ManifestPayload) -> dict[str, Any]:
    """回查清单内仍未发送的待补记录及原因。"""
    return TransferBatchService().backfill_unsent_report(payload.manifest_code)
