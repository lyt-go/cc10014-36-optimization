# 数仓分层治理平台

面向数据入库、清洗转换、分层建模、质量校验与血缘追踪的分层数仓治理平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 原始入库 | `raw_ingest` | 入库批次 | 批次编号、来源系统、文件格式 |
| 字段映射 | `column_mapping` | 映射规则 | 规则编号、源字段名、目标字段名 |
| 清洗策略 | `cleanse_rule` | 清洗策略 | 策略编号、目标字段、空值处理 |
| 去重配置 | `dedup_config` | 去重配置 | 配置编号、业务键列、排序字段 |
| 贴源层管理 | `ods_layer` | 贴源表 | 表编号、来源系统、表名称 |
| 明细层管理 | `dwd_layer` | 明细表 | 表编号、父层来源、表名称 |
| 汇总层管理 | `dws_layer` | 汇总表 | 表编号、统计粒度、汇总维度 |
| 维度建模 | `dim_model` | 维度模型 | 模型编号、维度名称、自然键列 |
| 快照管理 | `snapshot_mgr` | 数据快照 | 快照编号、目标数据集、快照时间 |
| 分区治理 | `partition_mgr` | 分区策略 | 策略编号、目标表名、分区字段 |
| 质量稽核 | `quality_check` | 稽核规则 | 规则编号、校验表名、校验字段 |
| 血缘追踪 | `lineage_track` | 血缘链路 | 链路编号、上游表名、下游表名 |
| 补数调度 | `backfill_job` | 补数任务 | 任务编号、目标表名、起始日期 |
| 表级比对 | `table_compare` | 比对任务 | 任务编号、左表名称、右表名称 |
| 结构注册 | `schema_registry` | 结构定义 | 定义编号、表名称、字段名称 |
| 数据发布 | `data_publish` | 发布版本 | 版本编号、数据集名称、发布范围 |
| 转换编排 | `transform_dag` | 转换节点 | 节点编号、输入表名、输出表名 |
| 冷热归档 | `cold_archive` | 归档策略 | 策略编号、目标表名、冷数据阈值 |

### 冷分区归档批次

冷热归档按“冷分区归档批次表”落库，批次表是归档结果与恢复的唯一依据：

- 批次表字段：批次编号、策略编号、目标表名、分区边界、归档路径、可恢复标记、归档结果、分区清单。
  字段缺失、边界与归档记录不一致时整体回滚，批次表与归档记录都不写入，边界与空态保持原样。
- 缺日期分区（`dt` 为空或无法解析）只进入空日期清单，不参与“最新分区”与保留期计算；
  只有缺日期数据时状态为“空态”，没有冷分区时为“没有”，无数据时为“暂无”。
- 批次按分区边界分批生成，同一冷分区并发提交只归档一次；失败/中断后可重试，已归档分区会被跳过并说明原因。
- 缺日期分区与保留期边界分区同时出现时登记冲突：批次表分别显示边界分区归档结果、大小分区归档结果与待修复集合，
  归档策略谁优先与处理顺序仍为“待确认”。
- 跨模块读取：快照管理通过 `GET /api/snapshot_mgr/archive-batches` 复核批次表；冷热归档页面提交后
  可在同一页面的批次面板查看边界、归档路径与恢复入口。

| 接口 | 说明 |
| --- | --- |
| `GET /api/cold_archive/{id}/plan` | 生成分批归档计划（批次边界、路径、可恢复标记、冲突） |
| `POST /api/cold_archive/{id}/archive` | 分批归档，`max_batches` 可模拟中断后续跑 |
| `GET /api/cold_archive/batches` | 冷分区归档批次表 |
| `POST /api/cold_archive/batches/{批次编号}/restore` | 按批次表登记路径恢复 |
| `GET /api/partition_mgr/{id}/partitions` | 分区边界、保留期边界与空日期清单 |
| `GET /api/snapshot_mgr/archive-batches` | 快照管理跨模块读取批次表 |
| `POST /api/snapshot_mgr/expired/archive` | 归档过期快照并回读可恢复标记 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
