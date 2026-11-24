import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---------------------------------------------------------------------------
// 补传链路专用接口：窗口缺失筛选、更正、清单回查、批次领取/差量发送/归属对照
// ---------------------------------------------------------------------------

async function postJson<T>(path: string, body: Record<string, unknown>): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  if (!response.ok) throw new Error('补传链路请求失败')
  return (await response.json()) as T
}

export type BackfillScreenResult = {
  manifest_code?: string
  manifest_id?: number
  window_code: string
  window_version?: string
  missing_count?: number
  overlap_count?: number
  overlaps?: { record_key: string; owner_window: string; reason: string }[]
  artifact_path?: string
  error?: string
}

export type BackfillReference = {
  manifest_code: string
  window_code: string
  window_version: string
  window_status_at_screen: string
  close_summary: Record<string, unknown>
  sent_scope: { window_code: string; sent_keys: string[]; batches: unknown[] }
  record_keys: string[]
  error?: string
}

export type RecordTrace = {
  record_key: string
  state: string
  reason?: string
  owner_window?: string
  owner_manifest?: string
  processed_by?: { batch_code: string; window_code: string; chunk_index: number }[]
  ledger_trail?: unknown[]
}

export function screenWindowMissing(windowCode: string, extraMissing: string[] = []) {
  return postJson<BackfillScreenResult>('/api/backfill/windows/screen-missing', {
    window_code: windowCode,
    extra_missing: extraMissing,
  })
}

export function applyWindowCorrection(
  windowId: number,
  recordKey: string,
  correctedFields: Record<string, unknown>,
) {
  return postJson<{ record_key: string; revision?: number; error?: string }>(
    '/api/backfill/windows/correction',
    { window_id: windowId, record_key: recordKey, corrected_fields: correctedFields },
  )
}

export async function loadManifestReference(manifestCode: string): Promise<BackfillReference> {
  const response = await request(`/api/backfill/manifests/${encodeURIComponent(manifestCode)}/reference`)
  if (!response.ok) throw new Error('清单引用结果读取失败')
  return (await response.json()) as BackfillReference
}

export function reopenManifest(manifestCode: string) {
  return postJson<{ manifest_code: string; status: string; window_version?: string; error?: string }>(
    '/api/backfill/manifests/reopen',
    { manifest_code: manifestCode },
  )
}

export function claimBackfillManifest(manifestCode: string, targetCode = 'TARG-0001') {
  return postJson<{
    batch_code?: string
    batch_id?: number
    manifest_code?: string
    error?: string
    conflicts?: { record_key: string; holder: unknown }[]
  }>('/api/backfill/batches/claim', {
    manifest_code: manifestCode,
    target_code: targetCode,
  })
}

export function transferBackfill(batchId: number, chunkSize = 3) {
  return postJson<{
    batch_code: string
    manifest_code: string
    records_transferred: number
    sent_keys: string[]
    chunks: { chunk_index: number; record_keys: string[] }[]
    error?: string
  }>('/api/backfill/batches/transfer', { batch_id: batchId, chunk_size: chunkSize })
}

export function traceBackfillRecord(recordKey: string) {
  return postJson<RecordTrace>('/api/backfill/records/trace', { record_key: recordKey })
}

export function loadUnsentReport(manifestCode: string) {
  return postJson<{
    manifest_code: string
    total: number
    unsent_count: number
    unsent: { record_key: string; state: string; reason: string }[]
    error?: string
  }>('/api/backfill/manifests/unsent-report', { manifest_code: manifestCode })
}
