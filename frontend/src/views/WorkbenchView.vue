<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="isBackfill" class="backfill-panel" data-testid="backfill-panel">
      <h3>补传链路操作</h3>
      <div class="backfill-row">
        <label class="filter-item">
          <span>闭合窗口编号</span>
          <input v-model="backfill.windowCode" data-testid="bf-window-code" placeholder="如 EXTR-SOUR-0001-…" />
        </label>
        <button class="btn primary" data-testid="bf-screen" type="button" @click="screenMissing">
          筛选缺失并生成清单
        </button>
      </div>
      <div class="backfill-row">
        <label class="filter-item">
          <span>清单编号</span>
          <input v-model="backfill.manifestCode" data-testid="bf-manifest-code" placeholder="如 BACK-…" />
        </label>
        <button class="btn" type="button" @click="showReference">回查引用版本/已发范围</button>
        <button class="btn" type="button" @click="claimManifest">领取清单</button>
        <button class="btn" type="button" @click="showUnsent">未发送原因</button>
        <button class="btn ghost" type="button" @click="reopen">重新打开</button>
      </div>
      <div class="backfill-row">
        <label class="filter-item">
          <span>补传批次 ID</span>
          <input v-model="backfill.batchId" data-testid="bf-batch-id" placeholder="列表中的 id" />
        </label>
        <button class="btn primary" type="button" @click="sendBackfill">生成差量分片并发送</button>
        <label class="filter-item">
          <span>源记录主键</span>
          <input v-model="backfill.recordKey" data-testid="bf-record-key" placeholder="如 R-SHARED" />
        </label>
        <button class="btn" type="button" @click="traceRecord">归属对照</button>
      </div>
      <pre v-if="backfill.output" class="backfill-output" data-testid="bf-output">{{ backfill.output }}</pre>
    </div>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  claimBackfillManifest,
  createEntry,
  downloadEntries,
  listEntries,
  loadManifestReference,
  loadUnsentReport,
  moduleMeta,
  reopenManifest,
  runAction as applyAction,
  screenWindowMissing,
  traceBackfillRecord,
  transferBackfill,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const isBackfill = props.moduleKey === 'backfill_manifest'
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: rows.value.filter((row: EntryRow) => row.status === meta.statuses[1]).length },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row: EntryRow) => row.status === meta.statuses[meta.statuses.length - 1]).length,
  },
])
const statusSummary = computed(() =>
  meta.statuses.map((status: string) => ({
    status,
    count: rows.value.filter((row: EntryRow) => String(row.status) === status).length,
  })),
)

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field: string) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

// -- 补传链路专用操作（仅补传清单模块展示） --
const backfill = ref({
  windowCode: '',
  manifestCode: '',
  batchId: '',
  recordKey: '',
  output: '',
})

function renderOutput(payload: unknown) {
  backfill.value.output = JSON.stringify(payload, null, 2)
}

async function screenMissing() {
  try {
    const result = await screenWindowMissing(backfill.value.windowCode.trim())
    renderOutput(result)
    if (!result.error && result.manifest_code) {
      backfill.value.manifestCode = result.manifest_code
    }
    await reload()
  } catch (error) {
    renderOutput({ error: error instanceof Error ? error.message : '筛选失败' })
  }
}

async function showReference() {
  try {
    renderOutput(await loadManifestReference(backfill.value.manifestCode.trim()))
  } catch (error) {
    renderOutput({ error: error instanceof Error ? error.message : '回查失败' })
  }
}

async function claimManifest() {
  try {
    const result = await claimBackfillManifest(backfill.value.manifestCode.trim())
    renderOutput(result)
    if (result.batch_id) backfill.value.batchId = String(result.batch_id)
    await reload()
  } catch (error) {
    renderOutput({ error: error instanceof Error ? error.message : '领取失败' })
  }
}

async function sendBackfill() {
  try {
    renderOutput(await transferBackfill(Number(backfill.value.batchId)))
    await reload()
  } catch (error) {
    renderOutput({ error: error instanceof Error ? error.message : '发送失败' })
  }
}

async function traceRecord() {
  try {
    renderOutput(await traceBackfillRecord(backfill.value.recordKey.trim()))
  } catch (error) {
    renderOutput({ error: error instanceof Error ? error.message : '归属对照失败' })
  }
}

async function showUnsent() {
  try {
    renderOutput(await loadUnsentReport(backfill.value.manifestCode.trim()))
  } catch (error) {
    renderOutput({ error: error instanceof Error ? error.message : '未发送回查失败' })
  }
}

async function reopen() {
  try {
    renderOutput(await reopenManifest(backfill.value.manifestCode.trim()))
    await reload()
  } catch (error) {
    renderOutput({ error: error instanceof Error ? error.message : '重新打开失败' })
  }
}

onMounted(reload)
</script>
