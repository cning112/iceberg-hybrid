# Iceberg 混合部署与云镜像（BLMS）设计说明（含性能优化与扩展考量）


## 1. 背景与目标

在不完全依赖云控面的前提下，构建**Iceberg 表的主写入在 on‑prem**、**云端（GCP/AWS）只读镜像**的混合部署方案：

- 云控面宕机时，on‑prem 仍可照常读写。

- 云侧（BigQuery、云上 Spark/Trino）读取一个**延迟可控**的只读镜像。

- 保证**单一真相源（SoT）**与**提交一致性**。

- 增强**GC 协同、历史保留策略、回压机制、成本优化与初始化流程**。

---

## 2. 总体架构概览

```
        ┌────────────────────────────────────────────────────────────────────┐
        │                            On‑Prem 侧                              │
        │                                                                    │
        │  Spark/Flink/Trino ──► REST Catalog(SOT)+Postgres ──►  对象存储     │
        │         (写/读)          (原子提交/指针)             (MinIO/S3兼容) │
        │                                           │                        │
        │                                           │差分+并行复制             │
        │                                           ▼                        │
        │                                     复制器(HA) ──► 写 _inprogress   │
        │                                                        校验OK→_ready│
        └────────────────────────────────────────────────────────────────────┘
                                                   │  对象复制+marker
                                                   ▼
        ┌────────────────────────────────────────────────────────────────────┐
        │                               云 侧                                │
        │  云存储(GCS/S3)  ◄────── 对象副本 + _ready/vN.marker               │
        │                                                                    │
        │   GCS/S3 事件(Pub/Sub or EventBridge)                            │
        │                                                                    │
        │   同步器(follower, 幂等/CAS) ──► BLMS(镜像表指针) ──► BigQuery/引擎    │
        │                                   (只读)            (查询)          │
        └────────────────────────────────────────────────────────────────────┘
```

---

## 3. 关键组件与职责

### 3.1 REST Catalog（SoT） + Postgres

- 维护**命名空间/表注册**、**当前 metadata.json 指针**、**乐观并发/CAS 提交**。

- 所有写入统一经由该 Catalog；**唯一真相源**。

### 3.2 On‑Prem 对象存储

- 存放数据文件、manifest、manifest‑list、metadata.json。

- 作为复制源；配合版本化与生命周期策略。

### 3.3 复制器（Replicator，HA）

- 从 SoT 的“上次镜像快照 vK”到“目标快照 vN”做**差分复制**（只传新增/必要对象）。

- **顺序**：先 data/manifest → 后 metadata.json → 最后**两阶段 marker**：`_inprogress/vN.marker`校验成功后晋升为
  `_ready/vN.marker`。

- **并行/限速**：文件分桶并行复制；带宽与并发受控；失败重试与断点续传。

### 3.4 Marker（哨兵文件）目录

- 位置：`gs://<bucket>/<ns>/<table>/_ready/vN.ready.json`（以及`_inprogress/`）。

- 语义：**镜像可读的原子发布信号**；follower 只认`_ready`。

### 3.5 同步器（Follower，幂等）

- 触发：对象存储事件（GCS Notifications/S3 EventBridge）或轮询`_ready/`。

- 动作：读取 marker → 轻量校验（HEAD metadata.json/校验和）→ 调用**BLMS 提交 API（CAS）**推进镜像表指针到 vN。

### 3.6 BLMS（BigLake REST Catalog）镜像表

- 在 BLMS 注册“镜像表”；**只读**。BigQuery/云上引擎通过 BLMS 读取云端镜像。

- 不参与同一张表的写入，避免双 Catalog 写冲突。

---

## 4. 时序与控制流（关键路径）

1. 写入引擎生成新数据与`vN.metadata.json`，调用 SoT 的**commit**（CAS）→ SoT 指针切到 vN。

2. 复制器从 vK→vN 计算**差分集**，并行复制 data/manifest → 复制`vN.metadata.json`。

3. 写`_inprogress/vN.marker`→ L0 快速校验通过 → 晋升为`_ready/vN.marker`。

4. 云存储事件触发同步器（或轮询发现`_ready`）→ 同步器 CAS 更新 BLMS 指针到`vN`。

5. BigQuery/云上引擎通过 BLMS 看到最新镜像；on‑prem 引擎始终读 SoT。

---

## 5. 一致性与可靠性设计

- **单一写入点**：仅 SoT Catalog 可提交。

- **两阶段 marker**：避免 zombie marker/部分复制误发布。

- **CAS 推进**：同步器提交时带 expected_prev，防止并发覆盖。

- **幂等与重试**：重复推进无害。

- **网络分区恢复**：复制器恢复后按 SoT 当前指针重算差分。

### 5.1 垃圾回收 (GC) 协同

- **问题**：on‑prem SoT 执行`expire_snapshots`删除过期快照/文件后，如何保证云端查询不受影响？

- **策略**：

    - SoT GC 生成**待删除清单**（object URIs + snapshot refs）。

    - Replicator 将此清单同步到云端 GC 服务。

    - 云端 GC 引入**延迟窗口**（例如 T+7 天），在确认对象未被任何活动查询引用后再执行删除。

    - 保留期内，云端可能保存多余对象，但避免长时查询失败。

- **安全性**：云端 GC 必须验证对象在 BLMS 活跃快照中是否仍被引用。

---

## 6. 性能优化策略

### 6.1 减少重复复制

- **快照差分**：解析`vK`/`vN`的 manifest‑list 与 manifests，聚合`status=ADDED`的对象集合，仅复制新增。

- **目的端去重**：复制前 HEAD/校验和比对；维护目的端对象索引（bucket inventory/Bloom filter）减少 HEAD。

- **快速前滚（Fast‑Forward）**：滞后超阈值时从 vK 直接跳 vN，复制 vK→vN 的差分，跳过中间版本的发布。

- **写入侧控小文件**：提升目标文件大小、周期性 compaction，降低对象数量。

- **服务端复制**：S3 Server‑Side Copy / GCS Rewrite，配合多段校验与 CRC32C。

### 6.2 平衡校验成本与延迟

- **分级校验**：

    - **L0 快速发布**：存在性+大小/CRC 抽样校验，合格即晋升`_ready`，降低端到端时延。

    - **L1 扩展校验（后台）**：完整 HEAD manifests、抽样校验大对象哈希；自动补复制+告警。

    - **L2 周期审计**：夜间对账可达闭包，验证引用一致性。

- **自适应抽样**：`sample_count = clamp(ceil(total_manifests * p), k_min, k_max)`；按规模调节成本。

### 6.3 大版本阻塞化解

- **并行/流水线**：按大小/分区分桶并行；将 data→metadata→marker 切成 Stage，前置“足够完成”即可推进后置阶段。

- **前滚阈值**：`mirror_lag_seconds > T`或“待传对象 > X 万”→ 启动 fast‑forward，只发布最新 vN。

- **分片推进**：内部分片并行推进，但对外仍以`_ready`原子发布整表版本。

- **GC 解耦**：发布与垃圾清理分离（T+1/T+7 运行`expire_snapshots`/orphan 清理）。

- **回压（Backpressure）**：依据吞吐/失败率自适应调整并发、分片大小与是否前滚。

### 6.4 回压与拥塞控制 (Backpressure)

- **问题**：云端复制或 BLMS 提交持续失败/延迟时，如何避免无限堆积？

- **策略**：

    - **复制层回压**：Replicator 自适应降低并发/速率（基于失败率和`mirror_lag_seconds`）。

    - **写入层保护**：当滞后超过 SLA 阈值，可向上游写入任务发信号（Spark/Flink checkpoint hook 或 API），限制新快照生成速率。

    - **优先级调度**：优先复制最新快照，老版本按 GC/保留策略延后或丢弃。

- **监控指标**：`replication_queue_depth`、`commit_backlog_size`。

### 6.5 成本优化

- **对象索引**：利用云存储**Inventory Service**定期生成对象清单，减少高频 LIST/HEAD 成本。

- **小文件优化**：复制器端可做**batch/concatenation**，减少 PUT 请求数量。

- **出口带宽**：利用**S3 Server‑Side Copy / GCS Rewrite**优化跨云传输，减少本地出口费用。

- **差分优先**：避免重复复制已存在对象。

---

## 7. 历史快照策略与 Fast‑Forward 限制

- **Fast‑Forward 策略**：允许直接从 vK → vN，跳过中间版本，提升追赶效率。

- **限制**：

    - 云端镜像将**缺失中间快照**，无法完整支持历史时间旅行。

    - 若业务需要完整的历史查询，则必须逐版本复制，不能使用 fast‑forward。

- **建议**：

    - 在文档中明确：当前云端镜像是否支持“全量历史时间旅行”。

    - 如果仅需“最新版本可查询”，fast‑forward 是最佳选择。

---

## 8. 监控与 SLO

- **核心指标**：

    - `mirror_lag_seconds`（SoT 指针 vs BLMS 指针滞后）

    - `replication_throughput_bytes_per_sec`/`files_per_sec`

    - `replication_error_rate`/`retry_count`

    - `marker_promotion_latency_seconds`

    - `blms_commit_latency_seconds`/`commit_conflict_count`

- **SLO 建议**：P95 镜像滞后 < 10–15 分钟；复制失败率 < 0.5%。

- **告警**：滞后>阈值、连续提交冲突、marker 孤立、审计失败。

---

## 9. 安全与权限

- **最小权限**：

    - 复制器：云桶写（含`_inprogress/_ready`）、读源桶；

    - 同步器：云桶读 + BLMS“更新指针”权限；

    - BigQuery/云引擎：云桶只读 + BLMS 读。

- **分离职责**：复制与指针推进由不同身份完成，降低误操作面。

---

## 10. 运维与对账

- **对账任务**：周期性对比 SoT 的`current_snapshot_id`与 BLMS 当前版本；若 BLMS 落后且无`_ready`，触发补复制/重试推进。

- **孤立 marker 处理**：发现`_ready`对象缺失依赖 → 降级删除`_ready`，保留`_inprogress`并重试复制。

- **事故演练**：断云/断网演练，验证 on‑prem 读写不中断与云侧恢复追平。

- **GC 对账**：周期性对比 SoT GC 待删除列表与云端对象，确认延迟窗口到期后删除。

---

## 11. 配置参数建议（可调旋钮）

| 类别  | 参数                         | 建议初值            |
| --- | -------------------------- | --------------- |
| 抽样  | `p` 抽样比例                   | 1–3%            |
| 抽样  | `k_min / k_max` manifests  | 100 / 2000      |
| 并行  | `max_concurrency`          | 64–256（按带宽/CPU） |
| 并行  | `rate_limit`               | 峰值带宽的 70–80%    |
| 前滚  | `lag_threshold_seconds`    | 600–900         |
| 校验  | `enable_L0_quick_publish`  | true            |
| 校验  | `enable_L1_async_audit`    | true（低峰）        |
| 告警  | `mirror_lag_alert_seconds` | 900             |

---

## 12. 引导/初始化 (Bootstrapping)

- **问题**：已有大表首次同步，数据量可能 PB 级，单靠网络复制耗时/成本过高。

- **策略**：

    - 使用云厂商提供的**物理迁移设备**（AWS Snowball、GCP Transfer Appliance）做一次冷启动。

    - 或者 Replicator 提供**全量模式**，分批并行上传，结合 checksum 校验，允许多周跑完全量复制。

    - 初始化完成后，再切换到**差分复制模式**，保证持续追赶。

---

## 13. Marker 与事件

### 13.1 Marker 文件结构（建议）

```json
{
  "table": "<ns>.<table>",
  "version": "vN",
  "metadata_uri": "gs://bucket/ns/table/metadata/vN.metadata.json",
  "manifest_list_uri": "gs://bucket/ns/table/metadata/snap-N-manifest-list.avro",
  "manifest_count": 123,
  "created_at": "2025-09-02T00:41:22Z",
  "producer": "replicator-01",
  "checksum": "sha256:ab12..."  
}
```

### 13.2 目录规范

```
<ns>/<table>/metadata/...
<ns>/<table>/_inprogress/vN.marker.json
<ns>/<table>/_ready/vN.ready.json
```

---

## 14. 伪代码骨架

### 14.1 复制器（差分+并行+两阶段 marker）

```python
# 计算差分
delta = list_added_files(from_snapshot=vK, to_snapshot=vN)
# 目的端索引以减少 HEAD
dest_index = load_dest_index()
# 并行复制（按大小分桶）
copy_in_parallel(filter_not_exists(delta, dest_index), max_concurrency, rate_limit)
# 复制 metadata/manifest-list/manifests
copy_metadata_and_manifests(vN)
# L0 快速校验
if quick_check_ok(vN):
    write_marker("_inprogress", vN)
    if extended_checks_ok_async(vN):
        promote_marker_to_ready(vN)
    else:
        schedule_background_audit(vN)
else:
    retry_or_alert(vN)
```

### 14.2 同步器（事件驱动 + CAS 提交）

```python
# 触发于 _ready/vN.ready.json Finalize 事件
sot_meta = get_sot_current_metadata(ns, table)      # gs://.../vN.metadata.json
blms_meta = get_blms_current_metadata(ns, table)
if blms_meta == sot_meta: return  # 幂等
assert gcs_exists(sot_meta)       # 轻量校验
blms_commit(expected_prev=blms_meta, new=sot_meta)  # CAS
verify(get_blms_current_metadata(ns, table) == sot_meta)
```

---

## 15. 迁移与落地步骤

1. 在测试项目部署 SoT（REST Catalog+Postgres）与复制器/同步器；

2. 选一张非关键表：

    - 冻结旧写入 →`register_table`到 SoT → 从 vK→vN 复制到云端 → 写 marker → 推进 BLMS；

3. 多引擎读写回归（on‑prem 引擎+BigQuery）；

4. 完成监控/告警/对账与演练 → 渐进式推广到全量表。


- **大表初始化**：建议先使用全量模式或物理设备完成冷启动，再启用差分复制。

---

## 16. 风险清单与缓解（更新版）

- **GC 删除风险**：采用延迟窗口 + 对账确认机制。

- **历史丢失风险**：Fast‑Forward 需在文档明确是否支持完整时间旅行。

- **复制阻塞**：回压策略避免无限 backlog。

- **高成本**：Inventory Service/批量写/Server‑Side Copy 降低费用。

- **初始化难**：冷启动工具 + 全量模式解决。

---

---

## 16. GC 协同与跨环境安全删除

**目标**：SoT 侧`expire_snapshots`后，云端副本**既不过删**正在被云查询引用的对象，也**不长期积累**垃圾。

### 16.1 基本原则

- **删除分两地两阶段执行**：SoT 产生“候删清单（delete set）”，云端在**保留期（grace window）**之后再删。

- **引用优先**：以**Catalog 指针 + 可达闭包**为准，永不删除仍被任一“对外可见快照/引用（tag/ref）”引用的对象。

### 16.2 术语与时间窗

- `T_sot_gc_grace`：SoT 本地 GC 的额外保留期（防止刚提交的长查询）。

- `T_cloud_gc_grace`：云端删除延迟（≥ 云端长查询 TTL / 时间旅行保留期）。

- `T_total_grace = T_sot_gc_grace + T_cloud_gc_grace`：端到端最小时窗。

### 16.3 清单与协议

1. **SoT 侧 GC 产出候删清单**：

    - 内容：对象 URI、大小、最后引用的`snapshot_id`、生成时间`ts`、可选校验和。

    - 存放：`<ns>/<table>/_gc/pending/<ts>.jsonl`（行式 JSON，便于增量处理）。

2. **复制器同步候删清单到云端**：与数据复制解耦，按批次传输到`.../_gc/pending/`。

3. **云端 GC 服务执行两阶段**：

    - **阶段 A：隔离标记**——将对象打标签`pending_delete=true`、`delete_after=ts+T_cloud_gc_grace`；（S3 Object Tagging / GCS
      Custom Metadata）。

    - **阶段 B：条件删除**——到期后再检查：

        - 该对象**不在**云端当前/历史**保留范围**的可达闭包内；

        - **无活跃查询**引用（可选：BigQuery/Trino 查询 TTL 低于门限）。

        - 满足则删除；否则续期`delete_after`。

4. **生命周期规则**：可配合存储生命周期策略按`pending_delete && now>delete_after`自动删除，GC 服务仅负责生成/维护标签与续期。

### 16.4 可选安全带

- **_trash/ 延迟区**：先移动到`_trash/yyyymmdd/`（对象存储无原子 rename，需复制后删，成本较高，按需启用）。

- **双人审批**：对大批量删除设审批阈值（>X TB 或 >Y 万对象）。

- **审计账本**：`_gc/logs/`记录批次、对象数、总大小、操作者与时间戳。

### 16.5 与时间旅行/历史保留的关系

- 若云端需要**完整时间旅行**（长历史窗口），将`T_cloud_gc_grace`设为 ≥ 云端历史保留；或干脆**禁止云端删除历史**、仅 SoT 清理。

- 若云端只需**“最新可读”**：`T_cloud_gc_grace`可较短（如 24–72h），快速回收成本。

---

## 17. Fast‑Forward 与历史快照策略

**问题**：Fast‑Forward（vK→vN 直接追赶）能提速，但会**不在云端发布**中间版本。

### 17.1 策略选项

- **模式 A｜最新优先（默认）**：云端仅保证**最新**，允许跳过中间版本；适合仪表盘/近线分析。

- **模式 B｜完整历史**：需要云端时间旅行：

    - 前台仍发布 vN；

    - **后台历史追赶 Job**按优先级补齐 vK+1..vN-1 的差分并补写`_ready`marker（不改变“最新”指针）；

    - 需确保 SoT 的`expire_snapshots`在`T_hist_catchup`期间**不删除**这些中间快照。

- **模式 C｜分层历史**：仅保留“日末/周末/里程碑”版本到云端，兼顾成本与可回溯性。

### 17.2 文档声明

在设计文档与用户手册中明确：

- 云端镜像的**历史完整性级别**（A/B/C），

- 对时间旅行/回放的 SLA，

- 当启用 Fast‑Forward 时的**中间版本可见性**约束。

---

## 18. 回压（Backpressure）与拥塞控制

**目标**：当复制/提交持续失败或滞后上升，系统**自适应降载**并向上游反馈，防止无限堆积。

### 18.1 核心信号

- `mirror_lag_seconds`、`pending_objects`、`replication_throughput`、`error_rate`、`blms_commit_latency`、
  `commit_conflict_count`。

### 18.2 分级动作（示例政策）

|等级|触发条件（示例）|复制器动作|同步器动作|上游写入动作|
|---|---|---|---|---|
|Green|`lag<5m` 且 `error_rate<0.1%`|常规并发|常规|常规提交|
|Yellow|`lag∈[5,15]m` 或吞吐下降|提升并发到上限、启用差分/跳 LIST、放宽 L0 抽样|缩短事件轮询|合并提交（延长 commit 间隔/批量合并）|
|Orange|`lag>15m` 或持续错误|启动 Fast‑Forward、限制小文件、动态限速|只推进“最新”|写侧启用 **flow‑gate**：当 `lag>阈值` 暂缓新快照发布/增加 compaction 比例|
|Red|`lag>60m` 或云端不可用|仅保留关键表复制、暂停历史追赶|暂停推进，避免空转|**写侧硬门控**：强制聚合/降频提交，或切只本地可见模式|

> 实现：复制器/同步器将指标上报 Prometheus；一个“策略控制器”定期计算级别并下发配置（并发、限速、是否 fast‑forward、写侧门控标志）。

### 18.3 写侧门控实现要点

- **提交节流**：将 writer 的 commit cadence（时间/文件数）提高到更大批次，减少快照数与对象数。

- **小文件抑制**：调大目标文件尺寸/强制合并；对近实时作业允许更大延迟以换更少对象。

- **可配置白名单**：关键表豁免或使用更高阈值。

---

## 19. 成本优化策略

- **Inventory 代替频繁 LIST**：

    - 启用**Bucket Inventory**（GCS 清单 / S3 Inventory）每日/每小时生成对象索引，复制器离线加载，减少线上 LIST/HEAD 成本；

    - 结合**Bloom Filter**/本地 KV 缓存，快速判断“目的端已存在”。

- **请求合并与速率整形**：

    - 控制每秒请求数（TPS），批处理 HEAD，优先使用**服务端复制**（S3 Copy/GCS Rewrite）。

- **小文件治理**：写侧 compaction、目标 256–512MB；复制侧不做“合并上传”（会破坏 Iceberg 引用）。

- **网络成本**：优先走专线/互联（Direct Connect/Interconnect）；尽量云端内重写避免公网出口；必要时使用**Transfer Service**
  的批量定价。

- **BLMS 提交成本**：指针推进轻量，可合并“多表提交”调用窗口，避免过度频繁调用。

---

## 20. 引导/初始化（Bootstrapping）

### 20.1 模式选择

- **全量迁移（冻结 + 一次性）**：短暂停写，完整复制校验后启用增量；最简单、时长取决于总数据量与带宽。

- **滚动迁移（分段/分区）**：先复制冷历史分区，再设“切换点”对热分区做增量；对大表更友好。

- **物理迁移设备**：超大规模时选用**AWS Snowball / GCP Transfer Appliance**将历史冷数据物理搬运到云，再接增量复制。

### 20.2 操作步骤（滚动迁移示例）

1. **准备**：禁用 SoT GC（或拉长窗口），建 BLMS 镜像表（空）。

2. **历史分区批复制**：按时间/分区并行复制到云端，不写`_ready`（避免暴露半历史）。

3. **校验**：对已复制分区做 L1 审计；修正缺失。

4. **切换点**：选定`cutover_ts`，从该时刻起启用增量复制与 marker 工作流；发布首个`_ready/vN`。

5. **补历史（可选）**：后台补齐 cutover 之前的遗漏版本，再逐步开放历史时间旅行。

6. **恢复 GC**：当云端历史完全可用后，再恢复 SoT 正常 GC 策略。

### 20.3 工时/带宽估算（指导）

- 粗略公式：`T ≈ 数据量 / (有效带宽 × 利用率)`；利用率受 RTT、并发、重试率影响。

- 先跑**带宽试验**与**小规模端到端演练**，再排期。

---