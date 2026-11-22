# 流变工坊 · 实时CDC与流式数据同步平台

面向数据库CDC事件捕获、变更日志解析、流式数据转换、维表关联补全、水位线管理、乱序容忍、结果落湖归档与端到端延监控的实时CDC与流式同步平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
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
| CDC数据源 | `cdc_source` | 链路源端 | 源端编号、数据库类型、连接地址 |
| 日志解析 | `binlog_parse` | 解析规则 | 规则编号、源端、日志格式 |
| 事件转换 | `event_trans` | 转换管道 | 管道编号、源端、事件类型 |
| 维表关联 | `dim_join` | 关联配置 | 配置编号、事实表、维度表 |
| 水位管理 | `watermark_mgr` | 水位线 | 水位编号、数据流、当前位点 |
| 乱序处理 | `out_of_order` | 乱序策略 | 策略编号、数据流、时间字段 |
| 结果落盘 | `sink_writer` | 落盘管道 | 管道编号、数据流、目标载体 |
| 延迟监控 | `lag_monitor` | 延迟指标 | 指标编号、数据流、源端位点 |
| DDL处理 | `ddl_handle` | DDL适配 | 适配编号、源端、DDL类型 |
| 断点恢复 | `checkpoint` | 检查点 | 检查点编号、数据流、保存位点、位点来源、位点格式、格式版本 |
| Schema演进 | `schema_evo` | 演进记录 | 记录编号、数据流、变更列 |
| 流去重 | `dedup_stream` | 去重窗口 | 窗口编号、数据流、去重键 |
| 湖仓提交 | `iceberg_commit` | 提交快照 | 快照编号、目标表、数据文件 |
| 小文件合并 | `compaction` | 合并任务 | 任务编号、目标表、文件数量 |
| 快照过期 | `expire_snap` | 过期策略 | 策略编号、目标表、保留数量 |
| 流聚合 | `metric_aggr` | 聚合窗口 | 窗口编号、数据流、聚合键 |
| 增量回填 | `backfill_run` | 回填任务 | 任务编号、目标表、起始位点 |
| 告警规则 | `alert_rule` | 告警策略 | 策略编号、监控指标、告警条件 |
| 数据比对 | `data_diff` | 比对任务 | 任务编号、源端、目标端 |
| 编排调度 | `task_schedule` | 调度计划 | 计划编号、关联链路、Cron表达式 |
| 流审计 | `stream_audit` | 审计记录 | 记录编号、数据流、操作类型 |
| 健康报告 | `stream_health` | 健康评估 | 评估编号、数据流、健康分数 |

## 保存位点：从无类型文本迁移到带来源与格式版本

存量检查点的 `保存位点` 只是无类型文本。迁移把可识别的文本转换成结构化保存位点：

```json
{
  "format": "saved-position",
  "format_version": 1,
  "source": "mysql",
  "source_format": "mysql-binlog-file-offset",
  "source_format_version": 1,
  "payload": {"file": "binlog.001234", "file_index": 1234, "offset": 891200},
  "raw": "binlog.001234:891200"
}
```

- MySQL 按 binlog `文件序号 + 字节偏移` 解释（`binlog.001234:891200`），
  PostgreSQL 按 `pg_lsn.<hi>/<lo>` 的 64 位 LSN 解释（`pg_lsn.0/3E82A00`）。
  两套坐标系分开比较，跨来源比较直接报错。
- 识别失败的文本（如 `checkpoint_after_overflow_3`）**原样留档**，
  不参与采用位点与读取起点计算。
- 已确认且未隔离的最高序号记录即“采用位点”；暂缓确认的记录保留在库中，
  恢复入口只按采用的已确认位点选读取范围，首个读取位置是采用位点之后一个字节。
- 回放成功由上层的进度提交登记；已成功回放的同一范围不会再次进入回填，
  中断后沿用原区间继续，不重复回放已完成部分。
- 旧写入端继续沿用原文本契约（`legacy-progress`），回滚时用原文本重新提交。

状态库默认在 `backend/.state/checkpoint-positions.sqlite3`，可用 `APP_STATE_DB`
覆盖。重启后读同一个库即可恢复已确认的旧版/新版位点。

```bash
# 可重启的本地夹具：建库 + 导入种子旧值
cd backend
.venv/bin/python -m app.scripts.checkpoint_fixture reset
.venv/bin/python -m app.scripts.checkpoint_fixture state order_events_cdc
.venv/bin/python -m app.scripts.checkpoint_fixture scenario   # 跑完整迁移/确认/回放/回滚
.venv/bin/python -m unittest discover -s tests -v
```

接口（`backend/app/routers/checkpoint_api.py`）：

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| POST | `/api/checkpoint-state/migrate` | 存量文本迁移（`dry_run` 可选） |
| POST | `/api/checkpoint-state/migrate/start` | 读取旧值后暂停迁移 |
| POST | `/api/checkpoint-state/migrate/complete` | 确认后完成迁移 |
| POST | `/api/checkpoint-state/legacy-progress` | 旧端原文本提交（`hold` 可暂缓确认） |
| POST | `/api/checkpoint-state/records/{seq}/hold` | 暂缓一条待确认位点 |
| POST | `/api/checkpoint-state/records/{seq}/release` | 单独放行一条位点 |
| POST | `/api/checkpoint-state/progress` | 新版进度提交，回报实际采用的格式与位点 |
| POST | `/api/checkpoint-state/restore` | 恢复入口：采用位点、首个读取位置、回填派发 |
| POST | `/api/checkpoint-state/rollback` | 按旧端契约回滚 |
| GET | `/api/checkpoint-state/positions` | 查看保存位点、暂缓、留档与已回放区间 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
