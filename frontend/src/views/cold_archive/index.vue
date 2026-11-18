<template>
  <section class="page" data-module="cold_archive">
    <header class="page-head">
      <div>
        <h2>冷热归档管理</h2>
        <p class="page-desc">维护归档策略的输入、执行约束与结果记录，为冷热归档提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记归档策略</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出冷热归档清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <section class="batch-panel" data-testid="batch-panel">
      <header class="batch-head">
        <div>
          <h3>冷分区归档批次表</h3>
          <p class="page-desc">归档后按批次边界、归档路径与可恢复标记复核归档策略结果。</p>
        </div>
        <div class="page-actions">
          <label class="batch-size">
            <span>每批分区数</span>
            <input v-model.number="batchSize" data-testid="batch-size" type="number" min="1" max="100" />
          </label>
          <select v-model="boundaryMode" data-testid="boundary-mode" class="batch-select">
            <option value="冷热阈值">冷热阈值边界</option>
            <option value="分区大小">分区大小边界</option>
          </select>
          <button class="btn primary" data-testid="run-batch-archive" type="button" :disabled="archiving" @click="runBatchArchive">
            {{ archiving ? '归档中…' : '分批归档' }}
          </button>
          <button class="btn" data-testid="reload-batches" type="button" @click="() => reloadBatches()">刷新批次表</button>
        </div>
      </header>

      <div class="batch-summary" data-testid="batch-summary">
        <span>批次 {{ batches.length }} 个</span>
        <span>可恢复 {{ restorableCount }} 个分区</span>
        <span v-if="batchMessage" data-testid="batch-message">{{ batchMessage }}</span>
      </div>

      <div v-if="batchResult" class="batch-result" data-testid="batch-result">
        <div class="batch-state">
          <span class="stat-label">归档状态</span>
          <strong data-testid="batch-result-state">{{ batchResult.状态 }}</strong>
        </div>
        <div class="batch-state">
          <span class="stat-label">边界分区</span>
          <strong data-testid="batch-boundary">{{ boundaryText }}</strong>
        </div>
        <div class="batch-state">
          <span class="stat-label">空日期参与保留期计算</span>
          <strong data-testid="empty-in-retention">{{ batchResult.空日期参与保留期计算 ? '是' : '否' }}</strong>
        </div>
        <div class="batch-state">
          <span class="stat-label">重试</span>
          <strong data-testid="batch-retryable">{{ batchResult.retryable ? '可直接重试' : '无需重试' }}</strong>
        </div>
        <div v-if="batchResult.空日期清单?.length" class="batch-empty" data-testid="empty-date-list">
          <span class="stat-label">空日期清单</span>
          <ul>
            <li v-for="item in batchResult.空日期清单" :key="item.partition">
              {{ item.partition }} · {{ item.row_count }} 行 · {{ item.reason }}
            </li>
          </ul>
        </div>
      </div>

      <div v-if="batchResult?.冲突" class="batch-conflict" data-testid="batch-conflict">
        <strong>缺日期分区与保留期边界分区冲突</strong>
        <div class="conflict-grid">
          <div>
            <span class="stat-label">边界分区归档结果</span>
            <p data-testid="conflict-boundary-result">
              边界 {{ batchResult.冲突.边界分区归档结果.边界分区 ?? '—' }} ·
              保留期边界 {{ batchResult.冲突.边界分区归档结果.保留期边界分区 ?? '—' }} ·
              结果 {{ batchResult.冲突.边界分区归档结果.归档结果 }} ·
              归档 {{ batchResult.冲突.边界分区归档结果.归档分区数 }} 个分区
            </p>
          </div>
          <div>
            <span class="stat-label">大小分区归档结果 / 待修复集合</span>
            <p data-testid="conflict-size-result">
              大小分区归档 {{ batchResult.冲突.大小分区归档结果.归档结果 }} ·
              待修复 {{ batchResult.冲突.待修复集合.空日期清单.length }} 个空日期分区
            </p>
          </div>
        </div>
        <p class="conflict-note" data-testid="conflict-order">归档策略谁优先与处理顺序尚待确认（{{ batchResult.冲突.处理顺序 }}）。</p>
      </div>

      <table class="data-table batch-table" data-testid="batch-table">
        <thead>
          <tr>
            <th>批次编号</th>
            <th>策略编号</th>
            <th>分区边界</th>
            <th>归档路径</th>
            <th>可恢复标记</th>
            <th>归档结果</th>
            <th>恢复</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="batch in batches"
            :key="batch.批次编号"
            :data-testid="`batch-row-${batch.批次编号}`"
            :data-restorable="batch.可恢复标记"
          >
            <td>{{ batch.批次编号 }}</td>
            <td>{{ batch.策略编号 }}</td>
            <td>{{ formatBoundary(batch.分区边界) }}</td>
            <td>{{ batch.归档路径 }}</td>
            <td>{{ batch.可恢复标记 }}</td>
            <td>{{ batch.归档结果 }}</td>
            <td class="row-actions">
              <button
                class="link"
                type="button"
                :data-testid="`restore-${batch.批次编号}`"
                @click="restore(batch)"
              >按批次恢复</button>
            </td>
          </tr>
          <tr v-if="!batches.length">
            <td colspan="7" class="empty-state" data-testid="batch-empty">暂无归档批次，提交归档后会显示边界与恢复路径</td>
          </tr>
        </tbody>
      </table>
    </section>

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
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
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
          <td :colspan="columns.length + 1" class="empty-state">暂无冷热归档数据，可先登记归档策略</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记归档策略</h3>
        <label v-for="field in columns" :key="field" class="create-field">
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
      <span>共 {{ total }} 条冷热归档记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/cold_archive'
const columns = ["策略编号", "目标表名", "冷数据阈值", "归档路径", "压缩算法", "归档格式", "保留天数", "恢复超时"]
const actions = ["提交校验", "标记生效", "停用策略"]
const statuses = ["草稿", "待校验", "已生效", "已停用"]
const metricNames = ["归档策略总数", "待校验策略", "已生效策略"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const batches = ref<any[]>([])
const batchMessage = ref('')
const batchResult = ref<any>(null)
const archiving = ref(false)
const batchSize = ref(5)
const boundaryMode = ref('冷热阈值')
const restorableCount = computed(() =>
  batches.value.reduce((sum, batch) => sum + (batch.分区清单?.length ?? 0), 0))
const boundaryText = computed(() => {
  const boundary = batchResult.value?.边界分区
  if (!boundary) return '—'
  if (boundary.起始分区 && boundary.结束分区) {
    return `${boundary.起始分区} ~ ${boundary.结束分区}（${boundary.分区数量} 个分区）`
  }
  return boundary.partition || boundary.分区 || '—'
})
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
  { label: '归档批次', value: batches.value.length },
])

function formatBoundary(boundary: any): string {
  if (!boundary) return '—'
  if (boundary.起始分区 && boundary.结束分区) {
    return `${boundary.起始分区} ~ ${boundary.结束分区}`
  }
  return String(boundary.partition ?? boundary.分区 ?? '—')
}

async function reloadBatches(strategyNo?: string) {
  const query = new URLSearchParams()
  if (strategyNo) query.set('strategy_no', strategyNo)
  try {
    const response = await request(`${ENDPOINT}/batches?${query.toString()}`)
    if (!response.ok) throw new Error('冷分区归档批次表读取失败')
    const payload = await response.json()
    batches.value = payload.items ?? []
    if (!batches.value.length) batchMessage.value = '暂无归档批次'
  } catch (error) {
    batchMessage.value = error instanceof Error ? error.message : '批次表读取失败'
  }
}

async function runBatchArchive() {
  archiving.value = true
  batchMessage.value = ''
  batchResult.value = null
  try {
    const target = rows.value.find((row) => row.status === statuses[2]) ?? rows.value[0]
    if (!target) throw new Error('没有可归档的归档策略，请先登记并标记生效')
    const params = new URLSearchParams({
      batch_size: String(batchSize.value),
      boundary_mode: boundaryMode.value,
    })
    const response = await request(`${ENDPOINT}/${target.id}/archive?${params.toString()}`, { method: 'POST' })
    const payload = await response.json()
    batchResult.value = payload
    batchMessage.value = payload.message ?? ''
    await Promise.all([reload(), reloadBatches()])
  } catch (error) {
    batchMessage.value = error instanceof Error ? error.message : '分批归档失败'
  } finally {
    archiving.value = false
  }
}

async function restore(batch: any) {
  batchMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/batches/${encodeURIComponent(batch.批次编号)}/restore`, { method: 'POST' })
    const payload = await response.json()
    batchMessage.value = payload.message ?? ''
    await reloadBatches()
  } catch (error) {
    batchMessage.value = error instanceof Error ? error.message : '恢复失败'
  }
}

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'cold_archive.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('冷热归档动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '冷热归档操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('归档策略列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '冷热归档列表读取失败'
  }
}

onMounted(() => {
  void reload()
  void reloadBatches()
})
</script>
