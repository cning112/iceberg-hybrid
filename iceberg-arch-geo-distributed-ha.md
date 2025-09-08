# 设计方案：地理分布式、高可用的 Iceberg 部署架构

**版本: 2.1**

**作者: Gemini (Reviewed by User)**

---

<details>
<summary><strong>目录 (Table of Contents)</strong></summary>

- [1. 概述与目标](#1-概述与目标)
    - [1.1 非目标（Non-Goals）](#11-非目标non-goals)
    - [1.2 术语与命名约定（Glossary）](#12-术语与命名约定glossary)
    - [1.3 术语一致性卡片（Cheat Sheet）](#13-术语一致性卡片cheat-sheet)
- [2. 架构总览](#2-架构总览)
- [3. 核心组件详述](#3-核心组件详述)
    - [3.1 全局事务性目录](#31-全局事务性目录)
    - [3.5 Nessie 部署拓扑（实例数量与角色）](#35-nessie-部署拓扑实例数量与角色)
    - [3.6 提交门槛（Replica Commit Gate）](#36-提交门槛replica-commit-gate)
    - [3.2 同步服务](#32-同步服务)
    - [3.3 数据移动器](#33-数据移动器)
    - [3.4 存储注册表 (Storage Registry)](#34-存储注册表-storage-registry)
        - [3.4.1 Storage Registry Schema 与契约（全局单一注册表）](#341-storage-registry-schema-与契约全局单一注册表)
        - [3.4.2 Registry 变更与一致性（Versioning & Atomic Rollout）](#342-registry-变更与一致性versioning--atomic-rollout)
- [4. 关键工作流](#4-关键工作流)
    - [4.1 写入工作流](#41-写入工作流)
    - [4.2 同步工作流 (以“美国同步服务”为例)](#42-同步工作流-以美国同步服务为例)
        - [4.2.1 阶段一：数据同步 (物理文件层)](#421-阶段一数据同步-物理文件层)
        - [4.2.2 阶段二：元数据同步与本地化 (逻辑层)](#422-阶段二元数据同步与本地化-逻辑层)
    - [4.3 读取工作流 (以“美国查询引擎”为例)](#43-读取工作流-以美国查询引擎为例)
- [5. 运维与成本](#5-运维与成本)
    - [5.1 故障切换与回切（摘要）](#51-故障切换与回切摘要)
        - [5.1.1 详细切换 Runbook（含一致性与飞行事务）](#511-详细切换-runbook含一致性与飞行事务)
    - [5.2 垃圾回收 (GC) 协同](#52-垃圾回收-gc-协同)
        - [5.2.1 触发与调用（Who / When / How）](#521-触发与调用who--when--how)
        - [5.2.2 GC 的 base 分支与可达性根（强制口径）](#522-gc-的-base-分支与可达性根强制口径)
        - [5.2.3 GC 作用域与安全边界（必须读）](#523-gc-作用域与安全边界必须读)
        - [5.2.4 GC 协同流程（分步）](#524-gc-协同流程分步)
        - [5.2.5 全局候删清单（来源与格式）](#525-全局候删清单来源与格式)
            - [5.2.5.1 它是什么](#5251-它是什么)
            - [5.2.5.2 存放位置](#5252-存放位置)
            - [5.2.5.3 内容字段（建议 Schema）](#5253-内容字段建议-schema)
            - [5.2.5.4 执行幂等性与状态机（Regional Executor）](#5254-执行幂等性与状态机regional-executor)
            - [5.2.5.5 如何消费](#5255-如何消费)
        - [5.2.6 GC 任务/服务（明确定义）](#526-gc-任务服务明确定义)
            - [5.2.6.1 组件与归属](#5261-组件与归属)
            - [5.2.6.2 触发方式（gc-producer）](#5262-触发方式gc-producer)
            - [5.2.6.3 输入（gc-producer）](#5263-输入gc-producer)
            - [5.2.6.4 输出（gc-producer）](#5264-输出gc-producer)
            - [5.2.6.5 算法步骤（gc-producer）](#5265-算法步骤gc-producer)
            - [5.2.6.6 伪代码（gc-producer）](#5266-伪代码gc-producer)
            - [5.2.6.7 消费者（gc-executor，各地域）](#5267-消费者gc-executor各地域)
            - [5.2.6.8 指标与告警（补充）](#5268-指标与告警补充)
        - [5.2.7 Orphan Files（孤儿文件）](#527-orphan-files孤儿文件)
    - [5.3 监控与告警指标](#53-监控与告警指标)
        - [5.3.1 目录层（Catalog / Nessie）](#531-目录层catalog--nessie)
        - [5.3.2 同步服务层（Sync / DAG）](#532-同步服务层sync--dag)
            - [5.3.2.1 端到端可读时间线（从写入到区域可读）](#5321-端到端可读时间线从写入到区域可读)
        - [5.3.3 存储/数据面（Storage / Data Mover）](#533-存储数据面storage--data-mover)
        - [5.3.4 告警规则（PromQL 示例，按需调整阈值）](#534-告警规则promql-示例按需调整阈值)
        - [5.3.5 指标与 SLO 对照表](#535-指标与-slo-对照表)
    - [5.4 成本优化](#54-成本优化)
        - [5.4.1 清单服务（Storage Inventory）详解](#541-清单服务storage-inventory详解)
            - [5.4.1.1 它是什么 & 为什么要用](#5411-它是什么--为什么要用)
            - [5.4.1.2 配置要点（建议）](#5412-配置要点建议)
            - [5.4.1.3 日期分区与路径规范（标准化层）](#5413-日期分区与路径规范标准化层)
            - [5.4.1.4 AWS S3 Inventory（示例）](#5414-aws-s3-inventory示例)
            - [5.4.1.5 GCS Storage Insights（Inventory 报告，示例）](#5415-gcs-storage-insightsinventory-报告示例)
            - [5.4.1.6 同步服务如何使用清单](#5416-同步服务如何使用清单)
            - [5.4.1.7 更高频清单的利弊（Pros & Cons）](#5417-更高频清单的利弊pros--cons)
- [6. 实施与迁移](#6-实施与迁移)
    - [6.1 冷启动 (Bootstrapping)](#61-冷启动-bootstrapping)
    - [6.2 安全与网络](#62-安全与网络)
        - [6.2.1 RBAC 权限矩阵（最小权限）](#621-rbac-权限矩阵最小权限)
- [7. 附录：与初始设计的对比分析](#7-附录与初始设计的对比分析)
    - [7.1 核心思想演进](#71-核心思想演进)
    - [7.2 详细差异对比](#72-详细差异对比)
    - [7.3 总结](#73-总结)
    - [7.4 同步 DAG 伪代码示例](#74-同步-dag-伪代码示例)

</details>

## 1. 概述与目标

本文档描述了一个旨在解决数据全球化挑战的、地理分布式、高可用的 Apache Iceberg 数据湖部署架构。

本设计的核心目标如下：

*   **地理分布式写入 (Geo-Distributed Writes)**: 允许位于任何地理位置的写入任务（如 Spark/Flink 作业）将数据写入其最近的存储系统，以实现最低的写入延迟。
*   **地理分布式读取 (Geo-Distributed Reads)**: 保证位于任何地理位置的查询引擎（如 Trino/Presto）都能从一个完整的、本地化的数据副本中读取数据，以实现最低的查询延迟和数据出口成本。
*   **高可用性 (High Availability)**: 系统的核心元数据服务具有高可用性，并为数据同步和读取路径提供故障隔离。
*   **明确的一致性模型（非全球强一致）**：**仅在主目录地域的写路径提供强一致性**；跨地域副本采用**异步复制/最终一致**，以**复制延迟 SLA**（而非严格共识）来约束可见性。**本设计明确不追求“全球单系统镜像的严格一致存储”。**

### TL;DR（Executive Summary）
- **Write Path（主写路径强一致）**：所有写入只经由 **Primary Catalog (Nessie + Primary DB)**，保证 `commit` 的线性一致性与顺序性。
- **Read Path（跨地域最终一致）**：各地域通过 **Catalog Replica（只读 DB）+ Replica Nessie** 读取 `main_replica_<region>` 分支，接受受控的 **replica visibility lag**。
- **Data Plane（数据面）**：每个地域通过 **Sync Service**（Airflow）+ **Data Mover**（Rclone）构建 **Storage Replica**，并做 **路径本地化** 后以 **CAS** 推进区域分支。
- **GC 协同（GC Coordination）**：在主目录执行 `expire_snapshots` 后，由 **GC Orchestrator** 生成 **全局候删清单 (Global Deletion Candidate List)**，经 DB 复制广播到各地域，由 **Regional GC Executor** 在 `delete_after` 之后对本地副本做安全删除（详见 5.2.3–5.2.6）。

### 1.1 非目标（Non‑Goals）

- 不构建“全球混合、严格一致”的元数据存储（不做跨广域网的强同步共识、非多主写入）。
- 不保证跨地域的“读你所写”（Read‑Your‑Writes）；该语义仅在主目录地域成立。
- 不在副本库执行任何写事务；副本仅承担读取与可见性传播职责。

### 1.2 术语与命名约定（Glossary）
- **主目录（Primary Catalog）**：承载 Nessie 的**主库**（可写），所有 `main` / `main_replica_<region>` 的创建与提交都发往此处。
- **目录副本（Catalog Replica）**：Nessie 连接的**只读目录数据库**（只读库）。用于跨地域读取与可见性传播；不在其上执行任何写事务。
- **目录副本实例（Replica Nessie Instance）**：部署在各地域、面向查询流量的 Nessie 服务实例；其背后连接目录副本（只读）。
- **存储副本 / 数据副本（Storage Replica）**：各地域对象存储中的**数据文件副本**（Parquet/Manifest/Metadata 等物理对象）。
- **区域分支（Regional Catalog Branch）**：`main_replica_<region>`，仅存在于**目录（Nessie）**中，用于提供“路径已本地化”的读取视图。
- **可见性滞后（Replica Visibility Lag）**：提交在主目录完成到在目录副本可见之间的时间差（由数据库复制决定）。

### 1.3 术语一致性卡片（Cheat Sheet）
> 一屏速览（统一口径，便于跨团队协作）。

- **Primary Catalog**（主目录）→ *Nessie + Primary DB（可写）*；全局**单写者（Single‑Writer）**所在。
- **Catalog Replica**（目录副本）→ *只读 DB*；通过**DB 异步复制**获得 `main`/`main_replica_*` 的可见性。
- **Replica Nessie**（目录副本实例）→ 连接**只读 DB** 的 Nessie 服务实例；仅对查询端提供只读 API。
- **Storage Replica**（存储/数据副本）→ 各地域对象存储中的**物理文件副本**（Parquet/Manifest/Metadata）。
- **Regional Branch**（区域分支）→ `main_replica_<region>`；由**同步服务**在**主目录**上以 **CAS** 推进；副本地域仅读取。
- **Replica Visibility Lag**（副本可见性滞后）→ `main@hash` 在副本库可见的延迟；SLO 指标。
- **Sync Service**（同步服务）→ Airflow/DAG + Rclone；负责差分复制、路径本地化、推进 `main_replica_*`。
- **GC Orchestrator**（gc‑producer）→ 主目录地域任务；在 `expire_snapshots` 成功后**生成**“全局候删清单”。
- **Regional GC Executor**（gc‑executor）→ 各地域任务；依据清单与 `delete_after` 在本地**安全删除**副本对象。
- **Global Deletion Candidate List**（全局候删清单）→ **唯一真相源**；结构化列出不可达对象、大小、地区副本路径、删除时点。

---

## 2. 架构总览

本架构的核心思想是将**元数据管理（控制平面）**与**数据同步（数据平面）**彻底分离。

*   **控制平面**: 一个高可用的**主目录服务 (Primary Catalog)**，负责接收所有写入事务。该目录的变更通过异步复制，同步到其他地区的只读副本。
*   **数据平面**: 一个去中心化的**同步服务网格**，负责物理数据文件的拉取和元数据的本地化。

```
                                  ┌──────────────────────────────────┐
                                  │      Global Transactional        │
                                  │ Catalog (Nessie + Hybrid HA Backend)│
                                  └──────────────────┬─────────────────┘
                                                     │ (Commits to 'main' branch)
                                                     │
                         ┌───────────────────────────┴───────────────────────────┐
                         │ (Write jobs commit metadata with absolute, hetero URIs) │
                         ▼                                                       ▼
┌──────────────────────────────────┐                          ┌──────────────────────────────────┐
│        Location: US-East         │                          │        Location: EU-Central      │
│                                  │                          │                                  │
│  [Write/Query Engines]           │                          │  [Write/Query Engines]           │
│           │                      │                          │           │                      │
│           ▼                      │                          │           ▼                      │
│  ┌───────────────────┐           │                          │  ┌───────────────────┐           │
│  │   S3 Storage      │◄─────────┼──────────────────────────┼──►│   S3 Storage      │           │
│  │ (us-east-1)       │  (Sync)   │                          │  │ (eu-central-1)    │           │
│  └───────────────────┘           │                          │  └───────────────────┘           │
│           ▲                      │                          │           ▲                      │
│           │ (Reads from local)   │                          │           │ (Reads from local)   │
│           │                      │                          │           │                      │
│  ┌───────────────────┐           │                          │  ┌───────────────────┐           │
│  │  Sync Service     ├───────────┘                          └───────────┤  Sync Service     │           │
│  │  (Airflow + Rclone)│                                                │  (Airflow + Rclone)│           │
│  └───────────────────┘                                                └───────────────────┘           │
│           │                                                                     │                      │
│           └────────────► Syncs from 'main', Advances 'main_replica_<region>' ◄────────────┘          │
│                                                                                                      │
└──────────────────────────────────┘                          └──────────────────────────────────┘
```

---

## 3. 核心组件详述

### 3.1 全局事务性目录

*   **服务层**: **Project Nessie**。
*   **后端实现 (核心挑战与权衡)**:
    *   **架构模式**: 为了在性能、稳定性、可用性和运维成本之间取得平衡，本设计推荐采用**主从异步复制**的数据库架构。
    *   **主集群 (Primary Cluster)**: 在一个主要地点（例如，美东）部署一个内部高可用的数据库集群（推荐 **CockroachDB** 或 **PostgreSQL with Patroni/Cloud HA**）。**所有 Nessie 的写入操作都只发往这个主集群**，从而保证写入路径的强一致性。
    *   **副本集群 (Replica Clusters)**: 在其他每个地区，部署一个只读的数据库副本。
    *   **复制机制**: 使用数据库层面的**异步复制**功能（例如，PostgreSQL 的流式复制或 CockroachDB 的 CDC 功能）来将主集群的数据同步到各地的副本集群。
*   **元数据后端设计取舍（非全球强一致）**:
    *   **单写者（Single‑Writer）模型**：所有 `main` 与 `main_replica_<region>` 的创建/提交**只**发往主目录（连接主库）。各地域通过**数据库异步复制**获得可见性。
    *   **一致性契约**：
        - **Linearizable**：仅对**主目录**上的提交成立（写后读一致、读读一致、顺序一致）。
        - **跨地域最终一致**：副本地域的可见性受 `catalog_db_replication_lag_seconds` 约束；不承诺远端的读你所写/单调读，除非流量粘附在主目录。
        - **分支可见性**：`main_replica_<region>` 的推进建立在主目录 `main@hash` 之上；副本看到的分支状态可能滞后于主目录的最新状态。
    *   **为何不做全球强一致**：
        - **广域网延迟/抖动**导致共识开销过高（p99/p999 写延迟与可用性显著下降）。
        - **运维复杂度**与**故障域扩大**（跨云/跨机房一处抖动拖垮全局写路径）。
        - **成本不可控**（同步共识对网络/算力要求高）。
    *   **SLA 建议**：
        - `replica_visibility_lag_seconds` P95 ≤ 60s（示例；按业务需要设定）。
        - 主目录 `commit_latency_ms` P95 ≤ 200ms（示例；取决于后端/拓扑）。
        - 明确 RPO ≤ 1min（由复制策略/队列长度保证）。
    *   **读语义提示**：
        - 需要“读你所写/单调读”的工作负载，**强制路由到主目录地域**；
        - 需要低成本/低延迟查询的工作负载，优先读取本地域 `main_replica_<region>`（接受滞后）。
*   **一致性语义澄清**:
    *   **写路径**: 在主集群所在地区，提供**强一致性**保证。
    *   **读路径**: 在副本集群所在地区，提供**最终一致性**。数据同步延迟（`replication_lag`）是一个核心的SLO监控指标。

> **重点**：本方案的“强一致”仅限于**主目录上的写路径**；跨地域是**最终一致**。我们**不采用**“全球单系统镜像的严格一致存储”（例如跨洲多主/同步共识数据库）。

*   **故障转移 (Failover)**:
    *   主集群的故障转移是一个重大的运维操作。本设计初期推荐采用**手动的、有计划的故障转移**流程，通过运维手册和脚本执行。这包括将一个副本集群提升为主、修改Nessie配置、以及更新DNS或服务发现记录。自动故障转移在广域网上风险极高，容易导致“脑裂”。
*   **分支策略**:
    *   `main` 分支: 主分支，在主集群上进行提交，包含异构URI。
    *   **区域分支（Regional Catalog Branch）**: `main_replica_us`, `main_replica_eur` 等副本分支: 每个地区一个，由各地的同步服务在**主目录**上推进（CAS），副本仅用于只读可见，只包含本地化路径。
*   **Nessie 层（副本地域只读/分支与 CAS 细节）**
    *   **只读强约束**：
        - **DB 层**：副本库启用只读角色与参数（PostgreSQL: `hot_standby=on` / 移除 DDL/DML 写权限；CockroachDB：仅开放 follower reads）。
        - **网络层**：通过安全组/VPC 路由限制，禁止写入组件连接到副本库；仅允许同步服务与查询端 `SELECT` 访问。
        - **Nessie 层**：副本地域的 Nessie 以**只读服务身份**对外提供查询；**唯一允许写副本分支**的是本地域同步服务账号（用于提交“本地化元数据”）。
    *   **分支命名与创建（每地域一支）**：
        - 命名规则：`main_replica_<region>`，例如 `main_replica_us`、`main_replica_eur`。
        - 创建位置（单写者）：在**主目录 Nessie 实例**上创建，父锚点为主目录内当前可见的 
`main@hash`；目录副本实例仅承担读取。
        - 使用 Nessie CLI（示例）：
          ```bash
          # 读取主目录实例内 main 的可见哈希
          nessie --endpoint $PRIMARY_NESSIE get ref main -o hash
          # 在主目录上创建区域分支（以可见哈希为 base）
          nessie --endpoint $PRIMARY_NESSIE branch create main_replica_us --base-hash $H_MAIN
          ```
        - 使用 REST API（示例，简化）：
          ```http
          # 在主目录实例上创建
          POST /trees/branch
          {"branch": {"name": "main_replica_us"}, "baseRefName": "main", "expectedHash": "<H_MAIN_VISIBLE_IN_PRIMARY>"}
          ```
    *   **CAS 提交流水线（单写者：在主目录实例上执行）**：
        1. 读取**主目录实例**中 `main` 的可见哈希：`H_main = getRef("main").hash`；
        2. 读取 `main_replica_<region>` 当前 HEAD（在主目录）：`H_prev = getRef("main_replica_<region>").hash`（若不存在则按上文在**主目录**创建）；
        3. 构造“路径本地化”的 `CommitOperation` 列表（指向本地域存储的 URI）；
        4. 执行 CAS 提交（对**主目录**）：`commit(branch="main_replica_<region>", expectedHash=H_prev, parentHashHint=H_prev, ops=[...])`；
        **说明**：`parentHashHint` 为**优化性提示**，应指向“当前分支的上一次提交”（`H_prev`）；正确性由 `expectedHash=H_prev` 保证。
       即使 `main@H` 在读→提交流程中前进，该提示过时也只会增加一次父链解析，不影响正确性；如冲突按第 5 步重试。
        5. 若返回冲突（HEAD 变化/父链不匹配）则刷新哈希重试（幂等）。
        6. 由数据库异步复制，将该提交传播到各地域的目录副本；区域查询端在复制完成后可见。
    *   **不在目录副本库写入**：采用**单写者**模型，区域分支的**创建与推进**均在主目录完成；各地域仅通过数据库复制获得可见性，无需任何“上行发布”。
    *   **（说明）无“上行发布”步骤**：本设计默认所有提交都在主目录完成；只有在未来引入本地目录写入（不推荐）时，才需要网关式上行发布机制。

### 3.5 Nessie 部署拓扑（实例数量与角色）
> 为避免歧义：**可以在全球部署多个 Nessie 服务实例**。Nessie 是无状态服务，真正的“单写者/一致性边界”在其**后端目录数据库**。本设计遵循**单写者主库 + 异步只读副本**的元数据后端模型。

**推荐拓扑**
- **写平面（Write Plane）**：
  - 在**主目录地域**部署 1~N 个 Nessie 实例（可横向扩展、Behind LB）。
  - 这些实例**连接主库**（Primary Catalog DB），**允许写入**（`commit`, `merge`, `branch` 等）。
  - 对外通过**全局写入端点**（如 `nessie-write.company.com`）暴露，仅用于写/管理流量。
- **读平面（Read Plane）**：
  - 在**每个地域**可部署 1~N 个 **目录副本实例（Replica Nessie）**，各自连接**目录副本库（只读 DB）**。
  - 这些实例**仅提供读取**（以及必要的 `getRef`/`getCommitLog` 等只读 API），供本地域查询引擎访问**区域分支 `main_replica_<region>`**。
  - 对外通过**本地读取端点**（如 `nessie-read.<region>.company.com`）暴露，配合就近路由（GSLB/Anycast）。

**路由与权限**
- 写入端（Spark/Flink 等 Writer）→ **固定路由**到写入端点（主目录地域），保证写路径强一致与顺序性。
- 查询端（Trino/Presto 等 Reader）→ **就近路由**到本地域读取端点，读取 `main_replica_<region>`；必要时可回退到写入端点读取 `main`（接受跨区延迟/出口成本）。
- **IAM/ACL**：
  - 写入端点仅授权“写角色”；读取端点仅授权“读角色”。
  - 禁止查询工作负载对写入端点使用写 API（通过网关/防火墙与 RBAC 双重约束）。

**故障与回退**
- 本地域读取端点不可用或滞后超阈值：
  1) 读取端**临时回退**到写入端点（跨区读）；
  2) 同步服务提速/启用 Fast‑Forward 以追平；
  3) 观察期结束后自动切回本地读取端点。
- 主目录地域宕机：按“5.1 故障切换与回切”流程**提升某目录副本库为主**，将写入端点**指向新主**，读取端点保持只读。

**不建议的拓扑（列举以免误解）**
- 在多个地域的 Nessie 实例**同时直连主库并开放写权限**：跨区写会放大时延/冲突域。
- 在目录副本库上开启写权限：会破坏复制链路与单写者模型。

### 3.6 提交门槛（Replica Commit Gate）

同步服务只有在满足以下条件时，才会对 `main_replica_<region>` 提交：
1) 目标 `main` 提交在**副本目录数据库中可见**（DB 复制已到达，读取到 `H_main_replica`）；
2) 该提交引用的数据文件**已全部在本地域对象存储落地**并通过 L0 校验（存在性 + 大小/CRC 抽检）；
3) 已完成路径本地化重写；
4) CAS 提交使用 `expectedHash=H_prev`，并携带 `parentHashHint=H_main_replica` 以对齐父锚点，避免并发覆盖与乱序。


**CAS 提交伪代码（示例）**
```python
# 伪代码：在主目录实例上推进区域分支（带有限重试）
def promote_to_regional_branch(nessie, region, main_target_hash, ops):
    br = f"main_replica_{region}"
    # 1) 读取主目录 main 可见哈希（或由上游传入 main_target_hash）
    H_main = nessie.get_ref("main").hash
    # 可选：断言 main_target_hash 已可见（部署时按需要开启）
    # assert is_ancestor(H_main, main_target_hash) or H_main == main_target_hash

    # 2) 读取/创建区域分支
    if not nessie.ref_exists(br):
        nessie.create_branch(br, base_hash=H_main)
    H_prev = nessie.get_ref(br).hash

    # 3) CAS 提交（expectedHash 防止并发覆盖；parentHashHint 对齐父锚点）
    for _ in range(5):
        try:
            # 说明：parent_hash_hint 应指向“当前分支的上一次提交”（即 H_prev），用于优化后端父链解析；
# 不应传入 H_main（主分支 HEAD），以免造成歧义。
            nessie.commit(branch=br, expected_hash=H_prev, parent_hash_hint=H_prev, operations=ops)
            return True
        except ConflictError:
            H_prev = nessie.get_ref(br).hash
            sleep(jitter_backoff())
    raise RuntimeError("CAS failed after retries")
```

> **CAS 语义澄清**：`parent_hash_hint` 是可选的父链定位提示（optimization hint），**不参与并发控制**。
> 正确性完全由 `expected_hash` 与分支 HEAD 比较决定；提示失效仅可能增加一次父链解析，不会破坏 CAS 结果。

**可观测性与标注字段（Replica Commit Properties）**
为精确衡量副本分支推进滞后，区域分支的每一次 CAS 提交应在 commit properties 中写入以下标注字段（键名可据实现调整，但需保持一致）：
- `origin_main_hash`: 对应的主分支提交哈希（作为“同一业务提交”的关联键）。
- `origin_main_created_ts`: 主分支提交的创建时间（UTC ISO8601）。
- `data_copy_end_ts`: 本次提交所覆盖的数据文件在本地域**全部复制完成**的时间（UTC）。
- `localize_end_ts`: 路径本地化与内部校验完成的时间（UTC）。
- `replica_commit_created_ts`: 区域分支提交创建时间（Nessie 生成，亦可由服务显式写入 UTC）。

> 这些字段用于计算多种滞后指标（见 5.3 监控与告警指标），同时便于排查：是复制慢、还是本地化/提交慢。

### 3.2 同步服务

*   **角色**: 核心职责分为：1) **差分数据同步**；2) **元数据本地化**；3) **垃圾回收（GC）协同**。
*   **实现**: 推荐使用 **Apache Airflow** 作为编排器。每个地区的 Airflow 自身也应配置为高可用模式（例如，多节点、CeleryExecutor），以避免其成为本地同步任务的单点故障。
*   **关键特性**:
    *   **差分复制**: 必须通过 Iceberg API 计算快照间的差异，只复制新增的文件，而非全量比对。
    *   **快速前滚 (Fast-Forward)**: 当副本延迟过大时，应能跳过中间版本，直接同步到最新的快照，以尽快追平。
    *   **回压机制**: 同步服务应监控自身的处理能力和延迟。当延迟超过阈值时，应能发出告警，并考虑向上游（如果可能）或运维人员发出“减速”信号。
    *   **提交门槛集成**：同步 DAG 在“数据落地 + L0 校验通过”后，读取**主目录实例**中 `main@hash` 并在主目录执行对 `main_replica_<region>` 的 CAS 提交；失败（409/412）自动重试，直至可见性达成或超时告警。
    *   **数据同步角色**：保证每个地区都拥有一份全量的**存储副本（Storage Replica）**。

### 3.3 数据移动器

*   **角色**: 由同步服务调用，负责物理文件的跨云、跨区域复制。
*   **实现**: 推荐使用 **Rclone**。它功能强大，支持所有主流云存储，且其 `copy` 操作是幂等的，非常适合此场景。

### 3.4 存储注册表 (Storage Registry)

*   **角色**: 一个全局配置，用于将存储路径的前缀映射到访问该存储所需的元数据（如 Rclone 远程配置名、凭证ID等）。这是同步服务能够访问其他地区存储的关键。
*   **实现**: 一个由所有同步服务共享的 JSON 或 YAML 配置文件。

#### 3.4.1 Storage Registry Schema 与契约（全局单一注册表）

> 设计原则：**全局只有一个 Registry**，由所有地域的同步服务共享与只读加载；不为每个地域维护一份独立副本，避免配置漂移与重复维护。
>
> 路由规则：默认策略为 **“current_region”**——即同一条规则在任意地域执行时，**目的前缀始终取该地域在 `regions` 中声明的 `local_prefix`**。如遇少数特例，再通过 `overrides` 指定“某些来源永远落到特定地域”。

**示例 (YAML v2)**:
```yaml
version: 2
regions:
  us-east:
    local_prefix: "s3://prod-us-bucket/"
    rclone_remote: "us-prod"
    write_role_arn: "arn:aws:iam::123:role/us-writer"
  eu-central:
    local_prefix: "s3://prod-eu-bucket/"
    rclone_remote: "eu-prod"
    write_role_arn: "arn:aws:iam::456:role/eu-writer"
  asia-south:
    local_prefix: "gs://prod-asia-bucket/"
    rclone_remote: "asia-prod"
    write_role_arn: "projects/abc/roles/asia-writer"

# 全局路径来源声明：列出所有可能出现的“源前缀”
sources:
  - "s3://prod-us-bucket/"
  - "s3://prod-eu-bucket/"
  - "gs://prod-asia-bucket/"

# 目的地策略：默认一条，表示“复制到当前地域的 local_prefix”
dest_policy:
  mode: "current_region"   # 也可扩展为 "specific_region"
  ensure_local_prefix: true  # 保护：计算出的目标必须落在当前地域的 local_prefix 下

# 可选特例：把某些来源强制落到指定地域（绕过 current_region）
overrides:
  - match_from: "gs://cold-archive/"
    to_region: "us-east"
    reason: "冷归档统一落 US，节省成本"
```

**字段说明**:
- `regions.*.local_prefix`：各地域**唯一**的本地对象存储前缀，作为落地与路径本地化的目标根。
- `regions.*.rclone_remote` / `write_role_arn`：数据移动与写入本地域存储所需的运行时信息。
- `sources`：**全局**可能出现的源前缀集合（可以跨多云多个桶）。
- `dest_policy.mode = current_region`：目的地一律取**当前运行地域**在 `regions` 里的 `local_prefix`；不再在规则里写死 `to`，从而避免为每个地域重复配置。
- `dest_policy.ensure_local_prefix`：开启后校验“目标 URI 必须以本地域 `local_prefix` 开头”，防止越权落地。
- `overrides[]`：小众例外（如冷数据集中存放、法律/合规要求），按最前缀匹配将来源强制落到某个 `to_region`。

**不变量与约束**:
- `regions.*.local_prefix` 彼此不得前缀重叠（避免歧义）。
- `sources` 中的每一项必须是规范化的前缀（统一带尾斜杠）。
- 启动时：
  1) 逐一校验 `regions` 与 `sources` 的唯一性与格式；
  2) 预构建最长前缀匹配索引（Trie/二分表）；
  3) 若存在 `overrides`，校验 `to_region` 必须在 `regions` 中且不与 `ensure_local_prefix` 冲突。

**映射规则解析（Longest‑Prefix）与使用流程**:
1) **差分发现**：解析 `main` 的 ADDED 文件列表，得到一批源 URI。
2) **归类**：若某 URI 已以当前地域的 `local_prefix` 开头 ⇒ **已本地**，无需复制/重写。
3) **匹配**：在 `overrides` 中按最长前缀找 `match_from`（若命中 ⇒ `dest = regions[override.to_region].local_prefix + suffix`）。未命中再到 `sources` 做最长前缀匹配：`dest = regions[current].local_prefix + suffix`。
4) **校验**：若启用 `ensure_local_prefix`，确认 `dest.startswith(regions[current].local_prefix)`；否则报错与告警。
5) **执行**：据此生成 `rclone copy` 计划与元数据路径本地化重写。

**解析伪代码**:
```python
def resolve_dest(uri: str, current_region: str, registry: Registry) -> str:
    from urllib.parse import urlparse
    parsed = urlparse(uri)
    assert parsed.scheme in ("s3", "gs"), f"unsupported scheme: {parsed.scheme}"

    local_prefix = registry.regions[current_region].local_prefix

    def norm(p): 
        return p if p.endswith("/") else p + "/"

    local_prefix = norm(local_prefix)

    # 1) 已本地：直接返回
    if uri.startswith(local_prefix):
        return uri

    # 2) overrides：最长前缀 + **路径段对齐**（避免 data/ 与 data-archive/ 混淆）
    def segment_match(u, pfx):
        if not u.startswith(pfx): 
            return False
        return u == pfx or u[len(pfx)] == "/"

    ov = max((o for o in registry.overrides if segment_match(uri, norm(o.match_from))), 
             key=lambda o: len(norm(o.match_from)), default=None)
    if ov:
        dest_root = norm(registry.regions[ov.to_region].local_prefix)
        return dest_root + uri[len(norm(ov.match_from)):]  # 边界已对齐

    # 3) sources：同样要求路径段对齐
    src = max((s for s in registry.sources if segment_match(uri, norm(s))), 
              key=lambda s: len(norm(s)), default=None)
    if not src:
        raise ValueError(f"No mapping for {uri}")

    dest = local_prefix + uri[len(norm(src)):]
    if registry.dest_policy.ensure_local_prefix and not dest.startswith(local_prefix):
        raise ValueError(f"Dest {dest} not under local_prefix {local_prefix}")
    return dest
```

**为何采用全局单一注册表**:
- **配置管理建议**：推荐使用 GitOps 流程统一管理 Storage Registry 配置，并由同步服务在启动时从集中式配置服务（如 Consul、Etcd）或标准化的 S3/GCS 路径安全地拉取。
- **避免配置漂移**：不再为每个地域复制/改写一份 `to` 前缀。
- **减少重复**：同一来源只在 `sources` 中列一次，目的地由“当前地域”推导。
- **更易审计**：所有地域共读一份真相源；例外集中体现在 `overrides`，一目了然。

#### 3.4.2 Registry 变更与一致性（Versioning & Atomic Rollout）
**读写模型**：运行态仅支持只读加载（read‑only）；唯一写入者（Single Writer）是平台 GitOps 流水线。
**原子发布**：
 - 采用 versioned object 策略：每次发布写入新 Key（如 `registry/v2/registry-<generation>.json`），并原子替换指针文件 `registry/current.json`。
 - 指针文件包含 generation、checksum、valid_from（UTC）。
**客户端热更新**（Sync Service）：
 1) 定期拉取 current.json；若 generation 增大则加载新版本；
 2) 校验 checksum 后才切换；
 3) 新任务用新版本，旧任务用旧版直至完成（graceful reload）。
**并发与回滚**：
 - 并发发布通过 Git PR 串行化；
 - 回滚 = 将指针文件指回旧 generation。
**一致性与观测**：
 - 所有时间戳使用 UTC，依赖 NTP；
 - 导出指标：registry_generation、registry_reload_success_total、registry_reload_failure_total。

---

## 4. 关键工作流

### 4.1 写入工作流

1.  一个位于欧洲的 Spark 作业完成数据处理。
2.  它将生成的 Parquet 文件写入其最近的存储，即欧洲 S3 (`s3://my-company-eur/...`)。
3.  它向全局 Nessie 服务的 `main` 分支提交一个新的 commit，该 commit 的元数据中包含了新文件的完整绝对路径。

### 4.2 同步工作流 (以“美国同步服务”为例)

位于美国的 Airflow 同步服务按计划触发 DAG，其工作流严格分为两个连续的阶段：首先是数据同步，然后是元数据同步。

#### 4.2.1 阶段一：数据同步 (物理文件层)

此阶段的目标是确保所有被全局 `main` 分支引用的数据文件，在本地域**存储副本（对象存储）**中都存在一个物理副本。

1.  **计算差异**: 任务通过 Nessie 和 Iceberg 的 API，计算出 `main` 分支上自上次同步以来新增的**数据文件**的精确列表。
2.  **识别异地文件**: 遍历该列表，通过查询“存储注册表”，识别出所有文件路径不属于美国本地存储的“异地文件”。
3.  **执行物理复制**: 为每一个需要复制的异地文件，生成一个并行的、幂等的 `rclone` 任务，将其从源头（如欧洲S3）复制到本地的美国S3中。
4.  **确认完成**: 只有当一个批次（例如一个Nessie commit）所需的所有异地数据文件都被成功复制到本地后，才进入下一阶段。


#### 4.2.2 阶段二：元数据同步与本地化 (逻辑层)

此阶段的目标是为本地查询引擎提供一个路径完全本地化的、可供查询的元数据入口点。

1.  **触发条件**: 对应批次的数据同步任务全部成功完成。
2.  **检出全局元数据**: 从 `main` 分支最新的、已完成数据同步的 commit 中，检出其完整的 Iceberg 元数据树。
3.  **路径重写**: 在内存中遍历元数据树（特别是 manifest 文件），将其中所有的文件 URI 都重写为指向美国本地 S3 的路径。
4.  **提交本地化元数据（目录）**: 将这份完全“本地化”的元数据，作为一个新的 commit，提交到美国地区的**区域分支 `main_replica_us`** 上。

完成此阶段后，位于美国的查询引擎就能通过 `main_replica_us` 分支安全、高效地查询数据了。

> 实现参考：见附录 7.4 “同步 DAG 伪代码示例”。


### 4.3 读取工作流 (以“美国查询引擎”为例)

1.  位于美国的 Trino 引擎被配置为从 Nessie 的**区域分支 `main_replica_us`**读取数据。
2.  当用户发起查询时，Trino 从 Nessie 获取的所有元数据中，文件路径都已经是本地的 `s3://my-company-us/...` 路径。
3.  Trino 高效地从本地存储读取数据，完成查询。

---

## 5. 运维与成本

### 5.1 故障切换与回切（摘要）
- **RTO/RPO 建议**：示例目标 `RTO ≤ 30min`，`RPO ≤ 1min`（取决于 DB 复制延迟与运维流程）。
- **切换步骤**：冻结新写入 → 停止同步 → 审计 `main`/副本可见度 → 提升某副本库为主（更新 Nessie 指向）→ 更新服务发现/DNS → 灰度放开写 → 观察期与回滚预案。
- **回切步骤**：原主恢复后，以 CDC 回灌新主期间的变更；窗口内保持“单主写入”；数据对账通过后切回原主。
- **风险控制**：全程单点网关负责可能的“上行发布”；严禁多地域直接向主实例写，防止脑裂。
- **一致性承诺在切换期的变化**：切主期间，强一致只在“新主”成立后恢复；切换窗口内远端读取可能出现短暂非单调现象（需要运维公告与读路由限制）。

#### 5.1.1 详细切换 Runbook（含一致性与飞行事务）
1) **冻结写入**：网关/写端点进入只读或直接 503；记录冻结开始时间 `t_freeze`。  
2) **一致性审计**：  
   - 确认副本库复制位点已追至或超过目标 `main@H_target`（检查 `pg_stat_replication`/CDC 水位）；  
   - 对关键表抽样比对 `commit log` 与 `table pointer` 是否一致；必要时暂停异步任务直至追平。  
3) **切换主库**：  
   - 将某副本库**提升为主**（DB 层完成只读→可写切换）；  
   - **更新 Nessie 指向**：将写端点 `nessie-write.company.com` 的 JDBC/DSN 切到新主库（K8s ConfigMap/Secret 滚动、或 Consul/Etcd 服务发现切换，或 DNS 切换 TTL≤30s）；  
   - 校验 Nessie 健康与 `commit` 正常。  
4) **飞行中事务处理**：  
   - 切换前已受理但未完成的 `commit`：以审计日志为准进行**幂等重放**或**标记失败**；  
   - 确保 WAL/CDC 已在新主**完全重放**，避免“半提交”状态；  
   - 对外发布变更公告，要求 Writer 重试失败的提交。  
5) **解冻写入**：灰度放量，观察 `commit_latency_ms`、冲突率与复制拓扑（原主→新从）稳定后全量放开。  
6) **回切准备**：原主修复并回灌 CDC 之后，按同样步骤回切（保持“单主”原则）。


### 5.2 垃圾回收 (GC) 协同

#### 5.2.1 触发与调用（Who / When / How）

**Who（由谁触发）**
- **Catalog GC = 在主目录（Primary Catalog）上执行 Iceberg 的 `expire_snapshots`**。执行者是**主目录地域的“目录维护”作业**（Airflow DAG 或等价调度），使用 Iceberg API/CLI 连到 **Primary Catalog** 的表。
- **GC Orchestrator（gc‑producer）**与 `expire_snapshots` **在同一维护作业内**，作为紧随其后的 Task 执行（成功 => 产出候删清单；无变更 => 跳过）。

**When（何时触发）**
1) **定时触发（Recommended）**：每日 02:00 UTC（示例）运行 `catalog_maintenance_primary`；对指定 `tables_scope` 滚动执行 `expire_snapshots` → `produce_gc_candidates`。
2) **按需触发（Runbook）**：运维或数据负责人在大版本发布/分区落盘后手动触发 DAG；同样遵循 `expire_snapshots → produce_gc_candidates` 顺序。
3) **阈值触发（Event‑Driven，可选）**：当监控发现 `table_unreachable_bytes > XTB` 或 `metadata_file_count > 阈值` 时，通过告警 Webhook 触发一次维护 DAG。

**How（如何调用 & 时序）**
- `expire_snapshots` 仅**移除目录层引用**（不直接删物理对象），生成新的 `table metadata` 与提交（在 `main`）。
- 紧接着调用 **GC Orchestrator（gc‑producer）**：
  1) 读取主目录当前可见 `main@H_active`；
  2) 对每张表做**可达性分析**，计算 `unreachable = all_files − reachable(H_active)`；
  3) 将 `unreachable` 写入 **`gc_candidates`**（DB 表，随复制分发）与可选 `_gc/pending/*.jsonl`；
  4) 为每条记录计算 `delete_after = produced_at + grace_period`（如 P7D）。
- **区域侧** `gc‑executor` 周期读取 `gc_candidates`（副本库），当 `now() ≥ delete_after` 执行**本地副本**物理删除，并在 `gc_executions` 记录结果。

**时钟与区域滞后保护（Clock & Regional Lag Guards）**
- 统一时间源：produced_at/delete_after 使用主库 DB 时间（UTC），执行端允许 clock_skew_guard（±120s）。
- 区域滞后兜底：若某地域 replica_visibility_lag_seconds / regional_commit_lag_seconds 超过阈值（如跨洲 >1800s），区域执行器自动延长本地删除（regional_extra_grace，如 +P1D），并告警。
- 网络分区恢复补偿：复制恢复后，执行器先做一次 refresh candidates + verify HEAD，对迟到可见的对象推迟删除，避免误删。

**幂等与安全**
- `gc_candidates` 对 `file_uri` 设唯一约束（或 UPSERT），保证重复触发不产生重复项；
- 仅当**主目录** `expire_snapshots` 成功且确认 `refcount==0` 时才产出候删条目；
- 任何删除均按 `delete_after` 执行，且支持重试/审计与双人审批阈值（见 5.2.4/5.2.5）。


**DAG 示例（精确任务顺序）**
```
catalog_maintenance_primary (02:00 UTC)
  ├─ t1_list_tables
  ├─ t2_expire_snapshots(table=*)          # Catalog GC（在 Primary 执行）
  ├─ t3_produce_gc_candidates(table=*)     # GC Orchestrator（紧随其后）
  └─ t4_metrics_and_audit

iceberg_gc_regional (*/10 min per region)
  ├─ r1_fetch_candidates(delete_after<=now, region=<current>)
  ├─ r2_optional_verify(head/inventory)
  ├─ r3_delete(replica_uri)
  └─ r4_upsert_gc_executions
```

#### 5.2.2 GC 的 base 分支与可达性根（强制口径）

**结论先行**：GC 可达性分析的**唯一基线（base）**是 **主目录的 `main@H_active`**，再**并集**一组明确的**受保护引用（Protected Refs）**；
**明确排除**所有 `main_replica_<region>`（区域分支，**除非被显式列入 Protected Refs 白名单**）。

**为何排除区域分支？**
- `main_replica_<region>` 是对 `main` 语义的**本地化视图**（路径重写 + CAS 前滚），不引入“新的业务引用”。
- 若把区域分支纳入可达性根，GC 将变成“按地域保留”，导致任何一个落后的区域都**阻断全局清理**。
- 本设计以 **主目录 `main` + 受保护引用** 定义“业务上仍需保留的历史”，区域落后通过 `delete_after` 的**延迟窗口**与 SLO 来兜底。

**受保护引用（Protected Refs）**
- 形态：`tags` / `branches`，用于时间旅行、审计留存或回滚窗口。
    - **默认不包含** main_replica_*；如确有长期保留需求，可将某个特定区域分支显式加入白名单（不建议长期开启）。
- 配置：通过白名单或模式匹配（例如 `gc.protected_ref_patterns = ["release/*", "audit/*", "tag://*important*"]`）。
- 规则：只要对象仍被 `main` 或任一受保护引用可达，则**不进入候删清单**。

**可达性根的形式化**
```text
reachable = union( reachable(main@H_active),  reachable(protected_ref_1), ..., reachable(protected_ref_n) )
unreachable = all_files − reachable
```

**区域侧长查询如何避免误删？**
- 若区域存在“长查询/重放需求”，**必须在主目录上**创建一个**受保护 tag/branch** 锚定所需的 `main@H_x`；
- 或者使用**临时保护机制（Hold）**：区域在消费 `gc_candidates` 时对个别对象添加短期 `region_hold_until`（见 5.2.5 扩展字段），在到期前不执行删除。

**操作指引（Runbook）**
1) 时间旅行/合规留存 → 在主目录创建 `audit/<YYYY-MM>` tag 并纳入 `gc.protected_ref_patterns`；
2) 区域故障/大促期间的临时冻结 → 使用 `region_hold_until=now()+P3D` 延后删除；
3) 任何“需要长期保留”的诉求都应回归为**受保护引用**，而不是依赖区域分支的滞后。


> **为什么需要 GC？** Iceberg 的数据文件、删除文件、manifest 与元数据文件都遵循“不可变追加”模式，新提交只会写新文件，旧文件不会覆盖。这样保证了并发安全和时间旅行，但也带来一个问题：
**旧文件会无限累积**。如果没有垃圾回收，存储成本会持续膨胀、查询可能扫描到已过期的数据文件、长时间运行的查询也可能读到被删除的数据。GC
的目标是：在确保没有任何活跃快照或长查询引用这些文件后，**统一、安全地删除不再需要的物理对象**，避免存储浪费与查询错误。

> **术语先导（Global Deletion Candidate List）**：本设计在主目录完成 GC 后，会由 **GC Orchestrator** 生成一份**全局候删清单**（结构化记录不可达的 data/delete/manifest/metadata 对象、大小与地区副本路径等）。该清单作为**唯一真相源**随目录数据库的异步复制分发到各地域，供区域执行器按 `delete_after` 安全删除；**格式与落地位置**详见 **5.2.5**。

#### 5.2.3 GC 作用域与安全边界（必须读）

**核心强调**：GC 只会删除**“不可达（unreachable）”**的物理对象；**绝不会**删除仍被任何活跃快照/分支引用的对象。

**包含/不包含的对象类型**
- ✅ **会删除**（当且仅当不可达时）：
  - **数据文件（data files）**与**删除文件（delete files）**；
  - **Manifest** 与 **Manifest List** 文件；
  - 旧的 **table metadata JSON**（如 `v2.metadata.json` 等历史版本）。
- ❌ **不会删除**：
  - 任何仍被可达快照引用的对象；
  - 当前表的活动 `metadata.json`（当前指针所指版本）；
  - “孤儿文件（orphan files）”不在“不可达集”内，需要通过**单独的 Orphan 清理流程**处理（参见下文说明）。

**为何 Manifest/Metadata 也会被删？**
- 它们与数据文件一样，遵循 Iceberg 的不可变追加模型；当对应快照被 `expire_snapshots` 移除且不再被任何分支引用时，这些文件同样变为**不可达**，应进入候删清单，以避免无限累积。

**安全检查**（在生成候删清单时强制执行）
- 对候删对象做**引用计数/可达性验证**，确保 `refcount == 0`；
- 若对象出现在**任何活跃分支/标签**的可达集合中 ⇒ **排除**；
- 对于 `metadata.json`，额外校验**表指针**当前所指哈希不等于该版本；
- 采用**删除延迟窗口**（例如 7 天）+ 幂等删除与审计日志，预防长查询/回滚窗口风险。

**与 Orphan Files 的关系**
- 本节 GC 针对“**有过引用、现已不可达**”的对象；
- **Orphan Files** 是指**从未被目录引用**或由于异常/失败产生的“游离对象”，应通过**单独的策略**检测/删除（结合存储清单、写入端 marker 等），不由 `expire_snapshots` 的候删清单直接覆盖。


#### 5.2.4 GC 协同流程（分步）

以下步骤描述从主目录触发到各地域落地删除的端到端协同流程：

1.  **GC执行**: `expire_snapshots` 等清理操作**只能在主目录集群**上执行。
2.  **候删清单**：由**主目录地域的 GC Orchestrator（gc‑producer）**在 
	expire_snapshots 成功后**生成**“全局候删清单”，列出所有不再被任何活跃快照引用的物理对象（data/delete/manifest/metadata）。详见 **5.2.5**。
3.  **清单分发**: 这份清单被视为一种特殊的元数据，通过数据库的异步复制，分发到所有副本目录集群。
4.  **安全删除**: 每个地区的同步服务，在消费到这份清单后，会引入一个**安全延迟窗口**（例如7天），确保没有正在运行的长查询会引用这些文件。之后，它才会从**本地存储副本**中删除这些物理文件。
5.  **对象标签与生命周期**：为候删对象打标签 `pending_delete=true, delete_after=<ts>`（S3 Object Tagging / GCS Metadata），并结合生命周期策略在到期后自动删除；不同步服务仅维护标签与续期。
6.  **审计与审批**：将批次、对象数、总大小和操作者写入 `_gc/logs/`；超过阈值（>X TB 或 >Y 万对象）需双人审批。

#### 5.2.5 全局候删清单（来源与格式）

##### 5.2.5.1 它是什么
- **不是 Iceberg 自带的功能**：Iceberg 的 `expire_snapshots` 仅在当前表的元数据中移除引用，不会直接生成全局对象清单。
- **不是 Nessie 的功能**：Nessie 管理提交与引用，但不负责对象存储的物理文件删除。
- **是我们架构中定义的扩展产物**：当主目录执行 GC（如 `expire_snapshots`）时，由我们自定义的 **GC 任务/服务**生成一份结构化清单，作为全局删除计划。

##### 5.2.5.2 存放位置
- **数据库表**：推荐在主目录数据库内建一张 `gc_candidates` 表（或等价 CDC 流），这样能随 DB 异步复制自动分发到各地域的副本库。
- **对象存储目录**：可选方案，在对象存储 `_gc/pending/` 目录落地 JSONL/Parquet 文件，方便同步服务与审计工具消费。

##### 5.2.5.3 内容字段（建议 Schema）
- `origin_main_hash`: 触发清理的主分支 commit 哈希
- `table`: 表名（可选）
- `file_type`: data/delete/manifest/metadata
- `file_uri`（又名 `origin_uri`）：**主目录（Primary Catalog）位置的“规范 URI”**（Canonical）。仅记录主目录所在存储的路径；**不写入任何区域本地副本路径**。
- `size_bytes`: 文件大小
- `last_referenced_snapshot`: 最后一次引用该文件的 snapshot id
- `produced_at`: 清单生成时间
- `delete_after`: 安全延迟窗口结束后可删除的时间戳
- `regions[]`: 通过 Storage Registry 推导出的各地域副本路径（`replica_uri`）
  - 区域执行器删除时**只使用本地域的 `replica_uri`**；不会跨区/跨云直接删除 `file_uri`（主目录位置）。
- 可选：etag/crc32c、操作者、原因（如 unreachable_from_active_snapshots）
- 可选：`holds`（JSONB）：区域级临时保护，例如 `{"us-east": "2025-09-10T00:00:00Z"}` 表示在该时点前 **us-east** 不执行删除。
- 说明：
  - `file_type` 覆盖 data/delete/manifest/metadata 四类；
  - 清单**不包含**“孤儿文件（orphan files）”——它们通过独立流程处置。

- 可选：regional_status（JSONB）：按地域记录状态机（planned/deleting/deleted/missing/error，及 last_update_ts）。
- 可选：idempotency_key：用于跨重试去重（可取 <origin_main_hash>:<file_uri> 或候删记录自增 ID）。

**示例（JSON 行式）**
```json
{
  "origin_main_hash": "abc123...",
  "table": "sales.orders",
  "file_type": "data",
  "file_uri": "s3://prod-eu/orders/data/part-0001.parquet",
  "size_bytes": 268435456,
  "last_referenced_snapshot": 87654321,
  "produced_at": "2025-09-07T08:00:00Z",
  "delete_after": "2025-09-14T08:00:00Z",
  "regions": [
    {"region": "us-east", "replica_uri": "s3://prod-us/orders/data/part-0001.parquet"},
    {"region": "ap-south", "replica_uri": "gs://prod-ap/orders/data/part-0001.parquet"}
  ]
}
```

##### 5.2.5.4 执行幂等性与状态机（Regional Executor）
- 幂等删除：S3/GCS DELETE 对已不存在对象是幂等的；推荐条件删除（If-Match/Generation-Match）或先 HEAD 校验。
- gc_executions 以 (candidate_id, region) 唯一约束保证幂等，重复重试只更新状态。
- 状态机：planned → deleting → {deleted | missing | error}；missing 表示该地域未见副本，等待后续重试。
- 副本缺失：若某地域 replica_uri 未生成或 HEAD=404，置为 missing，不影响其他地域执行；可配置超期回收策略。
```

##### 5.2.5.5 如何消费
- 各地域同步服务定期从副本库或 `_gc/pending/` 读取候删清单。
- 筛选 `delete_after <= now()` 的记录：
  1) 解析出当前地域对应的 `replica_uri`（若缺失则按 Storage Registry 计算一次）；
  2) 可选：对 replica_uri 做清单/HEAD 抽检；对条件删除启用 If-Match/Generation-Match；若存在 holds[region] > now() 则跳过；否则删除。
  3) 将执行结果写入 `gc_executions` 表或 `_gc/logs/`，确保幂等与可审计。
  4) **禁止从本地域直接删除主目录位置**（`file_uri`/`origin_uri`）；主目录侧的删除由主目录地域的执行器负责（如需）。
  5) 根据结果更新 regional_status 并 UPSERT 到 gc_executions（以 candidate_id+region 唯一约束确保幂等）。

> **说明**：这样清单是“全局唯一真相源”，避免各地自己判断可达性造成口径不一致，也方便审计与合规。

#### 5.2.6 GC 任务/服务（明确定义）

##### 5.2.6.1 组件与归属
- **GC Orchestrator（gc‑producer，主目录地域，单写者）**：负责在 `expire_snapshots` 成功后，执行可达性分析并**写入候删清单**（DB 表 `gc_candidates`，以及可选的 `_gc/pending/*.jsonl`）。归属：平台/目录团队。
- **Regional GC Executor（gc‑executor，各地域）**：按计划消费 `gc_candidates`，在**本地域存储副本**执行物理删除并写回执行记录 `gc_executions`。归属：数据平台/同步团队。

##### 5.2.6.2 触发方式（gc‑producer）
- **事件触发**：作为主目录地域 Airflow DAG `iceberg_gc_primary` 的一个 task，紧随 `expire_snapshots` 之后执行。
- **或 Cron**：Kubernetes CronJob，例如 `0 2 * * *`（每日 02:00 UTC）。

##### 5.2.6.3 输入（gc‑producer）
- `tables_scope`：表范围（支持白名单/通配符）。
- `base_commit`：用于锚定可达性的 `main@H_active`（可从 Nessie 读取最新可见 `main`）。
- `grace_period`：安全延迟窗口（如 `P7D`）。
- `storage_registry`：用于推导各地域 `replica_uri` 的全局注册表（见 3.4.1）。

##### 5.2.6.4 输出（gc‑producer）
- **DB**：表 `gc_candidates`（主库，随复制分发到副本库）。
- **对象存储（可选）**：`s3://<ops-bucket>/_gc/pending/dt=YYYY-MM-DD/part-*.jsonl`（用于审计/离线消费）。

**`gc_candidates` 表结构（DDL 示例，PostgreSQL）**
```sql
CREATE TABLE IF NOT EXISTS gc_candidates (
  id BIGSERIAL PRIMARY KEY,
  origin_main_hash TEXT NOT NULL,
  table_name TEXT,
  file_type TEXT CHECK (file_type IN ('data','delete','manifest','metadata')),
  file_uri TEXT NOT NULL,
  size_bytes BIGINT,
  last_referenced_snapshot BIGINT,
  produced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  delete_after TIMESTAMPTZ NOT NULL,
  regions JSONB NOT NULL,        -- [{region, replica_uri}]
  checksum JSONB,                -- {etag, crc32c}
  UNIQUE (file_uri)
);
```

##### 5.2.6.5 算法步骤（gc‑producer）
1) 读取 `base_commit = getRef('main').hash`；
2) 对 `tables_scope` 中每张表执行可达性分析：`reachable = collect_reachable_files(base_commit)`；
3) 从表级对象索引取 `all_files`；计算 `unreachable = all_files - reachable`；
4) 对 `unreachable` 中每个 `file_uri`：
   - 依据 `storage_registry` 反推各地域 `replica_uri`；
   - 写入 `gc_candidates(file_uri, origin_main_hash, file_type, size_bytes, last_referenced_snapshot, delete_after, regions, checksum)`。

##### 5.2.6.6 伪代码（gc‑producer）
```python
def produce_gc_candidates(tables_scope, grace_period):
    H = nessie.get_ref('main').hash
    for tbl in tables_scope:
        reachable = iceberg.collect_reachable_files(tbl, H)
        all_files = iceberg.list_all_objects(tbl)  # 由元数据/清单索引提供
        for f in all_files - reachable:
            rec = build_candidate_record(f, H, grace_period, storage_registry)
            upsert_gc_candidate(rec)
```

##### 5.2.6.7 消费者（gc‑executor，各地域）
- 触发：Airflow DAG `iceberg_gc_regional` 或 Cron；周期例如 `*/10 * * * *`。
- 逻辑：
  1) 拉取 `delete_after <= now()` 且 `region = <current>` 的候删记录；
  2) 可选：用**清单**或在线 `HEAD` 做存在性/校验抽检；
  3) 调用对象存储 API 删除 `replica_uri`（带重试/节流/幂等）；
  4) 写回 `gc_executions(id, region, deleted_at, bytes, result)`；
  5) 失败记录进入死信队列，观察后重试。

**`gc_executions` 表结构（DDL 示例）**
```sql
CREATE TABLE IF NOT EXISTS gc_executions (
  id BIGSERIAL PRIMARY KEY,
  candidate_id BIGINT NOT NULL REFERENCES gc_candidates(id),
  region TEXT NOT NULL,
  deleted_at TIMESTAMPTZ,
  bytes BIGINT,
  result TEXT CHECK (result IN ('deleted','missing','error')),
  error TEXT
);
CREATE INDEX ON gc_executions (region, deleted_at);
```

##### 5.2.6.8 指标与告警（补充）
- 指标：`gc_candidates_total{region}`、`gc_deleted_total{region}`、`gc_executor_errors_total{region}`、`gc_executor_latency_ms`。
- 告警：`rate(gc_executor_errors_total[15m]) > 0`；`histogram_quantile(0.95, gc_executor_latency_ms_bucket) > 30000` 等。

#### 5.2.7 Orphan Files（孤儿文件）

> **定义**：**Orphan Files** 指**从未被目录（Catalog/Nessie/Iceberg 元数据）引用**、或因写入/复制失败而遗留在对象存储中的“游离对象”。它们不属于 `expire_snapshots` 生成的“不可达（unreachable）”集合，需由**独立流程**识别与清理。

**常见来源（Sources）**
- **失败/中断的写入**：如 Spark/Flink 任务在生成数据文件后未成功 `commit` 到 `main`。
- **临时/中间产物**：Compaction/Rewrite 阶段的中间文件、`_tmp/`、`_staging/` 等未被最终引用的对象。
- **复制侧残留**：Data Mover（Rclone）中断、重试异常导致的“目的端已落地但未被任何提交引用”的对象。
- **历史迁移遗留**：一次性冷启动/回灌中拷入但未建立目录引用的旧对象。

**清理原则（Principles）**
1) **与 Catalog GC 解耦**：孤儿文件不依赖 `expire_snapshots` 的结果；采用**独立扫描/对账**路径。
2) **可证伪**：删除前需有“**未被引用**”的充分证据链（清单/可达性/落地记录三方对账）。
3) **延迟与分层**：比 `unreachable` 更保守的 `grace_period_orphan`（例如 **P14D** 或更长），并优先清理**临时前缀**（`_tmp/`/`_staging/`）。
4) **幂等/可审计**：与 `gc_candidates`/`gc_executions` 类似，建立 `orphan_candidates`/`orphan_executions` 表与日志。

**判定方法（Detection）**
- **集合差分（Inventory × Reachable）**
  1) 以对象存储清单（S3 Inventory / GCS Storage Insights）为**全集** `Inventory(bucket/prefix, dt)`；
  2) 以某个 `main@H_active` 的**可达集（Reachable Set）**为**被引用集合**（可由 Iceberg 元数据/Manifest 索引计算，或从“区域分支已本地化文件清单”聚合得到）；
  3) 以**复制/落地记录**（Copy Plan/Completion）为辅助证据（证明对象曾由我们流程写入/复制过）。
  定义：`Orphan ≈ Inventory − Reachable`，并排除**清单新鲜度不足**和**迟到可见**的情况（通过 `inventory_freshness_lag_seconds` 与观测期窗口过滤）。

- **路径启发（Heuristics）**：对 `_tmp/`、`_staging/`、`compaction/tmp/` 等前缀设置更短的 `grace_period_orphan_tmp`（如 **P3D**），并优先清理。

- **版本化桶（Versioned Buckets）**：若开启多版本，孤儿判定需在 `IsLatest=true` 维度上进行，避免误删历史版本中仍被引用的对象。

**数据模型（DDL 示例）**
```sql
CREATE TABLE IF NOT EXISTS orphan_candidates (
  id BIGSERIAL PRIMARY KEY,
  bucket TEXT NOT NULL,
  key TEXT NOT NULL,
  size_bytes BIGINT,
  first_seen TIMESTAMPTZ NOT NULL,
  last_seen TIMESTAMPTZ NOT NULL,
  evidence JSONB,                -- {inventory_dt, etag/crc32c, source:'inv', notes}
  delete_after TIMESTAMPTZ NOT NULL,
  reason TEXT,                   -- e.g. 'not_referenced_by_catalog', 'tmp_prefix', 'copy_residual'
  UNIQUE(bucket, key)
);

CREATE TABLE IF NOT EXISTS orphan_executions (
  id BIGSERIAL PRIMARY KEY,
  candidate_id BIGINT NOT NULL REFERENCES orphan_candidates(id),
  deleted_at TIMESTAMPTZ,
  bytes BIGINT,
  result TEXT CHECK (result IN ('deleted','missing','error')),
  error TEXT
);
CREATE INDEX ON orphan_executions (deleted_at);
```

**识别算法（伪代码）**
```python
def detect_orphans(inventory_tbl, reachable_tbl, region, grace_orphan_days=14):
    inv = load_inventory(inventory_tbl, region, freshness_max_lag_hours=24)  # 保障清单新鲜
    reachable = load_reachable(reachable_tbl, region)  # 来自 Iceberg/Manifest 聚合或区域分支本地化清单
    for obj in inv:
        if obj not in reachable:
            reason = infer_reason(obj.key)
            upsert_orphan_candidate(
                bucket=obj.bucket, key=obj.key, size=obj.size,
                delete_after=now()+days(grace_orphan_days), reason=reason,
                evidence={"inventory_dt": inv.dt, "etag": obj.etag}
            )
```

**执行流程（Orphan Sweeper）**
- 调度：Airflow DAG `iceberg_orphan_sweeper`，建议每日运行；对 `_tmp/` 前缀可提高频率。
- 步骤：
  1) `o1_detect`: 运行**识别算法**，刷新/UPSERT `orphan_candidates`；
  2) `o2_verify`: 随机/分层抽检 `HEAD` 或与**区域分支已本地化文件表**交叉验证；
  3) `o3_tag`: 给对象打标签 `pending_orphan_delete=true, delete_after=<ts>`（S3 Object Tagging / GCS Metadata）；
  4) `o4_delete`: 到期后批量 `DELETE`，记录到 `orphan_executions`；
  5) `o5_audit`: 产出报表（桶/前缀/天维度）与成本归集。

**存储策略（Defense‑in‑Depth）**
- **生命周期策略（Lifecycle Policy）**：
  - 对 `_tmp/`, `_staging/`, `compaction/tmp/` 等前缀设置**自动过期**（例如 **7–14 天**）。
  - 在复制目的端也设置**相同或更严格**的生命周期，以避免跨云残留。
- **写入规范（Write Contract）**：
  - 写入/重写任务的**最终提交**前，所有临时文件必须位于上述临时前缀；提交成功后**原子重命名**或**清理**。
  - 失败路径（on‑failure hooks）中显式清理临时前缀。
- **审计与回溯**：
  - 对 “写入作业 ID ↔ 产生的对象 Key 列表” 建立**写审计表**；Orphan 检测时可据此排除“正在等待提交”的对象。

**与 `unreachable` 的关系**
- `unreachable`：**曾被引用**但现已不可达 → 由 **GC Orchestrator** 生成候删并分发。
- `orphan`：**从未被引用**或流程异常遗留 → 由 **Orphan Sweeper** 独立识别与删除。
- 两者**互补**：前者依赖 Catalog 状态，后者依赖存储清单与流程审计；都需**延迟窗口**与**幂等日志**。

### 5.3 监控与告警指标
#### 5.3.1 目录层（Catalog / Nessie）
> 目标：把“可观察项”从口号变成**可计算的量**。每个指标给出**定义/单位/采集方式/归属**，并在末尾列出**告警规则**。

- **`catalog_db_replication_lag_seconds`**  
  - **定义**：`now() - last_replayed_lsn_timestamp`（或等价的复制位点时间戳差）。  
  - **单位**：秒（s）  
  - **采集**：数据库视图/系统表（PostgreSQL: `pg_stat_replication`；Cockroach: `crdb_internal.cluster_sessions` + CDC 水位），以地域维度导出。  
  - **归属**：DBA/SRE（目录后端）  
  👉 **解读**：这是底层数据库复制位点的延迟，反映“日志已传输/重放到副本”的进度，不针对具体某个 commit hash。
- **`commit_latency_ms`**  
  - **定义**：主目录上一次 `commit` 请求的**服务端处理时长**（从接收到成功返回）。可导出 P50/P95/P99。  
  - **单位**：毫秒（ms）  
  - **采集**：Nessie/网关的 HTTP Server metrics（直方图）。  
  - **归属**：平台/目录服务
- **`nessie_api_latency_ms{operation}`**  
  - **定义**：Nessie API 的端到端延迟（`getRef`/`commit`/`diff` 等）。  
  - **单位**：毫秒（ms）  
  - **采集**：Nessie 服务端直方图；或 API 网关观测。  
  - **归属**：平台/目录服务
- **`nessie_commit_rate_per_min`**  
  - **定义**：`main` 分支提交速率（近 1 分钟滑窗内的提交数）。  
  - **单位**：次/分  
  - **采集**：轮询 Nessie 提交日志 / 审计流。  
  - **归属**：平台/目录服务
- **`replica_visibility_lag_seconds{region}`**  
  - **定义**：`main@hash` 在**目录副本库**变为可见的时间点与其在主目录创建时间点的差。  
  - **单位**：秒（s）  
  - **采集**：对照 `getRef("main")` 在副本库可见的目标 `hash` 与主目录中该 `hash` 的 `created_ts`（从审计/commit properties 读取）。  
  - **归属**：DBA/SRE（目录后端）  
  👉 **解读**：这是业务级别的可见性延迟，衡量某个具体的 main@hash 提交何时在副本库可见，直接对应查询端能否读到。

👉 SLO 衔接：GC 的 delete_after 应 ≥ P95 replica_visibility_lag_seconds + 安全裕度（如 +P1D）；跨洲部署应更保守。
- **`regional_commit_lag_seconds{region}`**  
  - **定义**：`replica_commit_created_ts - origin_main_created_ts`；反映“主提交发生 → 对应区域分支提交创建”的端到端滞后（**包含复制+本地化+CAS**）。  
  - **单位**：秒（s）  
  - **采集**：从区域分支的 commit properties 读取 `origin_main_created_ts` 与 `replica_commit_created_ts` 直接计算。  
  - **归属**：平台/同步联合
- **`commit_conflict_total{region}`**  
  - **定义**：区域分支 CAS 提交冲突（HTTP 409/412）总次数/速率。  
  - **单位**：次/分（rate）  
  - **采集**：同步服务对 CAS 调用的返回码统计。  
  - **归属**：平台/同步联合

**指标对照说明**:
  - `catalog_db_replication_lag_seconds`: 面向 **DBA/SRE** 的底层健康指标，用于诊断复制通道。
  - `replica_visibility_lag_seconds{region}`: 面向 **平台/查询侧** 的用户可见性指标，反映具体提交的可读延迟。
  - 两者结合使用，可以区分是“复制位点本身滞后”还是“副本应用/查询侧延迟”。

**可见性时间线（只看目录层）**
```text
 t0: 在主目录创建 main@H
  │
  │  （DB 复制与副本重放推进）
  ▼
 t1: 副本目录可见 main@H（复制位点覆盖到 H）
```

 指标对照：
 - 在任意时刻：`catalog_db_replication_lag_seconds = now() - last_replayed_lsn_ts`
 - 对于具体提交 H：`replica_visibility_lag_seconds(H) = t1 - t0`

#### 5.3.2 同步服务层（Sync / DAG）
- **`sync_job_end_to_end_lag_seconds{region,table}`**  
  - **定义**：从目标 `main` 提交创建（`origin_main_created_ts`）到**该提交在本地域完全可用**之间的时长；形式化为：`max(data_copy_end_ts, localize_end_ts, replica_commit_created_ts) - origin_main_created_ts`。  
  - **单位**：秒（s）  
  - **采集**：在 DAG 的 `t7_metrics_emit` 计算并上报。  
  - **归属**：数据平台（同步）
- **`regional_read_ready_lag_seconds{region,table}`**  
  - **定义**：从主目录提交创建（`origin_main_created_ts`）到**区域读者可消费**之间的时长；形式化为：  
    `regional_read_ready_lag_seconds = replica_commit_visible_ts(region) − origin_main_created_ts`。  
  - **说明**：`replica_commit_visible_ts(region)` 指 **区域副本库**首次可见该次 *区域分支提交* 的时刻（即副本 Nessie/DB 重放完成并可被 Reader 读取）。由于我们在推进区域分支前已确保 `data_copy_end_ts` 与 `localize_end_ts` 完成，因此**区域可读性**由“目录可见性”主导。  
  - **采集**：同步服务在提交区域分支后，**轮询区域副本 Nessie** 直到 `main_replica_<region>` 出现对应 `origin_main_hash` 的提交，记录可见时间；或从 DB 复制位点/审计流中读取首次可见时间。  
  - **归属**：平台/同步联合
- **`writer_to_regional_read_ready_lag_seconds{region,table}`**  
  - **定义**：从**写入端写完数据文件**（`writer_data_write_end_ts`，由写入作业上报）到**区域读者可消费**之间的时长；形式化为：  
    `writer_to_regional_read_ready_lag_seconds = replica_commit_visible_ts(region) − writer_data_write_end_ts`。  
  - **说明**：用于端到端体验评估（Writer → Reader）。当写入端无法可靠上报 `writer_data_write_end_ts` 时，可退化为以上 `regional_read_ready_lag_seconds` 指标。  
  - **采集**：Writer 在作业审计中上报 `writer_data_write_end_ts`（UTC），同步/观测面与 `replica_commit_visible_ts(region)` 对齐计算。  
  - **归属**：平台/数据团队联合
- **`copy_backlog_files{region}` / `copy_backlog_bytes{region}`**  
  - **定义**：已规划待复制但尚未完成的文件数/字节数。  
  - **单位**：个 / 字节（B）  
  - **采集**：来自复制计划表（元数据库）与完成表的差集。  
  - **归属**：数据平台（同步）
- **`rclone_throughput_bytes_per_sec{region}`**  
  - **定义**：跨存储复制的平均吞吐（1 分钟滑窗）。  
  - **单位**：B/s  
  - **采集**：rclone 作业导出的速率或 DAG 侧包裹统计。  
  - **归属**：数据平台（同步）
- **`fast_forward_activations_total{region}`**  
  - **定义**：触发 Fast-Forward 前滚的次数（用于追平）。  
  - **单位**：次（Counter）  
  - **采集**：同步服务状态机事件。  
  - **归属**：数据平台（同步）

**指标关系说明**：`sync_job_end_to_end_lag_seconds` 以 **区域分支提交创建时刻**（`replica_commit_created_ts`）为上限，反映同步与本地化阶段效率；而 `regional_read_ready_lag_seconds` 以 **区域副本库可见时刻** 为准，额外包含 **目录复制可见性** 的尾延迟（replica DB replay）。若两者差距显著，通常说明 **DB 复制/重放** 成为瓶颈。

#### 5.3.2.1 端到端可读时间线（从写入到区域可读）

```text
 tW: Writer 写完数据文件（writer_data_write_end_ts）
  │
  ├──(Writer 提交到主目录)────────────────────────────────────────────
  ▼
 t0: 在主目录创建 main@H（origin_main_created_ts）
  │
  │    （数据复制 + 路径本地化 + CAS 推进区域分支）
  ▼
 t2: 创建区域分支提交（replica_commit_created_ts；在主目录实例上）
  │
  │    （DB 异步复制到各区域副本库，重放提交日志）
  ▼
 t3: 区域副本库可见该提交（replica_commit_visible_ts(region)）
  │
  └──→ 区域读者可消费（Regional Readers Ready）
```

**关键等式**：
- `regional_read_ready_lag_seconds = t3 − t0`
- `writer_to_regional_read_ready_lag_seconds = t3 − tW`
- 由于推进区域分支前已满足 `data_copy_end_ts ≤ t2` 且 `localize_end_ts ≤ t2`，因此 `t3` 成为区域可读性的主导因素；若 `t3 − t2` 过大，排查 **catalog_db_replication_lag_seconds** 与副本重放性能。

**关系说明**：通常 `tW ≤ t0`；若 Writer 提交存在队列/网络抖动，则 `t0 − tW` 反映 **writer-side flush/commit 延迟**，不应与复制/本地化延迟混淆。

#### 5.3.3 存储/数据面（Storage / Data Mover）
- **`cross_region_data_transfer_bytes{region}`**  
  - **定义**：跨区域传输的累计字节数（可派生为 `*_gb`）。  
  - **单位**：字节（B）  
  - **采集**：rclone 统计 / 云厂商传输计量（Billing/CloudWatch/Storage Insights）。  
  - **归属**：FinOps/平台联合
- **`storage_api_calls_total{region,op}`**  
  - **定义**：对象存储 API 调用次数（`GET/LIST/HEAD/PUT/DELETE`）。  
  - **单位**：次（Counter）  
  - **采集**：SDK/代理导出或云监控（S3 CloudWatch、GCS Metrics）。  
  - **归属**：平台
- **`gc_candidates_total{region}` / `gc_deleted_total{region}`**  
  - **定义**：候删对象数量 / 已安全删除数量。  
  - **单位**：个（Counter/Gauge）  
  - **采集**：GC 清单产出与执行记录。  
  - **归属**：数据平台（同步）

- **`inventory_freshness_lag_seconds{bucket}`**  
  - **定义**：`now() - last_successful_inventory_ts`；某个清单数据集（按桶/地域）自上次**成功产出**以来的时间差，衡量“清单是否新鲜”。  
  - **单位**：秒（s）  
  - **采集**：从“标准化清单表/元数据”读取最近成功批次的 `dt/hour`（或 manifest 的完成时间），并按 `bucket`/`region` 维度导出。  
  - **归属**：平台/数据同步（与存储团队共担）

#### 5.3.4 告警规则（PromQL 示例，按需调整阈值）
- **目录复制滞后**  
  - **Yellow**：`catalog_db_replication_lag_seconds > 60` 持续 `5m`  
  - **Red**：`catalog_db_replication_lag_seconds > 300` 持续 `5m`
- **区域提交端到端滞后**  
  - **Yellow**：`histogram_quantile(0.95, sum(rate(regional_commit_lag_seconds_bucket[15m])) by (le,region)) > 600`  
  - **Red**：`histogram_quantile(0.95, sum(rate(regional_commit_lag_seconds_bucket[15m])) by (le,region)) > 1200`
- **复制积压**  
  - **Orange**：`sum(copy_backlog_bytes) by (region) > 5 * 1024^4` （> 5 TB） 持续 `10m`  
  - **Red**：增长斜率为正且 `rclone_throughput_bytes_per_sec < target_throughput_bytes_per_sec * 0.5` 持续 `15m`
- **提交冲突异常**  
  - **Warn**：`rate(commit_conflict_total[5m]) > 5`（次/分）  
  - **Red**：`rate(commit_conflict_total[5m]) > 20`
- **API 延迟异常**  
  - **Warn**：`histogram_quantile(0.95, sum(rate(nessie_api_latency_ms_bucket{operation="commit"}[5m])) by (le)) > 500`  
  - **Red**：同上 > 1000

- **清单新鲜度异常**  
  - **Yellow**：`inventory_freshness_lag_seconds{bucket=~".+"} > 6*60*60` 持续 `30m`  
  - **Red**：`inventory_freshness_lag_seconds{bucket=~".+"} > 24*60*60` 持续 `30m`  
  - 说明：若采用**每周**或其他频率，请相应上调阈值（例如 Weekly 可设 Yellow=3d、Red=7d）。

> **Runbook 指引**：
> 1) `catalog_db_replication_lag_seconds` 异常：先查网络与 DB 复制状态 → 评估是否触发读路由回退。  
> 2) `regional_commit_lag_seconds` 异常但 DB 正常：检查数据复制带宽/限速、`copy_backlog_*`、本地化执行日志，必要时启用 Fast‑Forward。  
> 3) `commit_conflict_total` 异常：核对 Gate 实现与 `expectedHash` 使用、并发度、是否有重复消费。  
> 4) API 延迟异常：抓取热点表与高频操作，评估扩容写端点或限流。


#### 5.3.5 指标与 SLO 对照表

| 指标名 | 定义 | SLO 目标 (示例) | 说明 |
| :--- | :--- | :--- | :--- |
| `catalog_db_replication_lag_seconds` | 主目录 → 副本目录 DB 复制延迟 | 同洲 P95 ≤ 60s；跨洲 P95 ≤ 180s | 保障副本可见性延迟受控；RPO ≤ 1min |
| `commit_latency_ms` | 主目录 commit 服务端延迟 | P95 ≤ 200ms | 写路径强一致性下的用户体验 |
| `replica_visibility_lag_seconds{region}` | `main@hash` 在副本库可见延迟 | P95 ≤ 60s | 保证副本读延迟上限；查询可用性关键 |
| `regional_commit_lag_seconds{region}` | 主 commit → 区域分支 commit 滞后 | 同洲 P95 ≤ 900s；跨洲/大表 P95 ≤ 1800s (30m) | 包含复制+本地化+CAS，端到端核心 SLO |
| `sync_job_end_to_end_lag_seconds{region,table}` | 主 commit → 本地域完全可用 | P95 ≤ 1200s (20m) | 表级观测；滞后过大需 Fast-Forward |
| `copy_backlog_bytes{region}` | 待复制字节数 | < 5TB（滚动 10m 窗口） | 过高提示带宽瓶颈或任务阻塞 |
| `rclone_throughput_bytes_per_sec{region}` | 跨区复制吞吐 | ≥ 目标带宽的 70% | 吞吐不足需检查网络/限速配置 |
| `commit_conflict_total{region}` | 区域分支 CAS 冲突次数 | < 5/min | 冲突高说明并发/父锚点逻辑需优化 |
| `storage_api_calls_total{region,op}` | 对象存储 API 调用次数 | 成本占比可控 | 与 FinOps 指标挂钩，监控 LIST/HEAD 频率 |

> 注：以上 SLO 值为示例，需结合具体业务场景与预算调整。正式运行前建议做基准测试与容量规划。


### 5.4 成本优化

*   **清单服务**: 对于源和目标存储，都应启用清单服务（如 S3 Inventory），同步服务可以利用这份清单来避免高成本的 `LIST` 操作。
*   **小文件合并 (Compaction)**: 必须在写入端实施积极的 Compaction 策略，将小文件合并为更大的、适合分析的文件（例如256MB-1GB），这能极大地降低需要复制的文件数量和API开销。
*   **Inventory + Bloom**：启用对象存储清单服务（S3/GCS Inventory）并配合本地 Bloom/索引，减少 `HEAD/LIST` 调用与误复制。

#### 5.4.1 清单服务（Storage Inventory）详解

##### 5.4.1.1 它是什么 & 为什么要用
- **存储清单**是对象存储周期性导出的**对象列表快照**（含 Key/Size/ETag/Checksum/StorageClass/VersionId 等元数据）。
- 作用：
  1) **替代高成本的 online LIST**（尤其是跨区/跨云）→ 同步服务用“离线清单”做差分；
  2) **快速对账/校验**（L0）→ 用 ETag/CRC32C 对比源/目的；
  3) **成本/配额分析** → 结合大小/存储类型统计；
  4) **GC 协同** → 候删对象与清单对账，降低误删风险。

##### 5.4.1.2 配置要点（建议）
- **范围**：覆盖所有参与复制的桶/前缀（源与本地）。
- **频率**：按云厂商支持的频率配置（常见为**每日**；部分产品支持**每周**或更高频率）。
- **格式**：优先 **Parquet**（便于 Athena/BigQuery 查询；压缩友好）。
- **字段**（至少）：`Key`, `Size`, `ETag`/`CRC32C`, `LastModified`, `StorageClass`, `IsLatest`, `VersionId`（若多版本开启）。
- **落地结构**：按**日期分区**与**源桶名**分层，例如：
  - S3：`s3://<inventory-bucket>/inventory/<source-bucket>/dt=YYYY-MM-DD/part-*.parquet`
  - GCS：`gs://<inventory-bucket>/reports/<source-bucket>/dt=YYYY-MM-DD/*.parquet`
- **保留期**：30–90 天；启用生命周期策略自动过期。
- **权限**：仅授予同步服务读权限；落地桶开启默认加密（KMS）。

##### 5.4.1.3 日期分区与路径规范（标准化层）
- **原始落地（raw）**：云厂商生成的清单文件在目标桶/前缀下交付，命名规则由厂商决定（通常附带交付日期/清单批次的目录或 manifest 文件）。
- **标准化落地（curated）**：建议在内部做一道轻量 ETL，将“原始清单”规范化为 **Hive 风格分区** 结构，便于按天/小时查询：
  - `s3://<inventory-curated>/inventory/<source-bucket>/dt=YYYY-MM-DD/part-*.parquet`
  - 可选小时分区：`.../dt=YYYY-MM-DD/hour=HH/part-*.parquet`
- **原因**：不同厂商/区域命名不一致；通过标准化层，查询与对账可以统一按 `dt`/`hour` 过滤，无需关心原始命名差异。
- **完成判定**：
  - 若供应商提供 `manifest.json`（如 S3 Inventory），以 manifest 作为该批次“完整性”判据再入仓。
  - 否则采用“到达窗口 + 计数阈值”的策略，避免读到不完整批次。
- **原始→标准化（伪代码）**：
```python
for batch in list_raw_inventory_batches():
    rows = read_raw(batch)  # CSV/Parquet
    dt = infer_dt(batch)    # 解析交付日期/文件名
    write_parquet_partition("curated.inventory", rows, partition={"dt": dt, **hour_opt(batch)})
    mark_committed(batch)
```

> **名词对齐（AWS vs GCP）**：
> - **AWS S3 Inventory**：Amazon S3 提供的“对象清单报表”功能。
> - **GCS Storage Insights（Inventory 报告）**：Google Cloud Storage 的同类能力，其产品名为 *Storage Insights*，其中包含“Inventory 报告”（对象清单）。
> 二者本质等价：都会按计划把某个桶/前缀下的对象元数据（Key/Size/ETag/Checksum/StorageClass 等）导出到你指定的目标位置。

##### 5.4.1.4 AWS S3 Inventory（示例）
```json
{
  "Id": "inv-us-prod",
  "IsEnabled": true,
  "IncludedObjectVersions": "Current",
  "Schedule": {"Frequency": "Daily"},
  "Destination": {
    "S3BucketDestination": {
      "AccountId": "1234567890",
      "Bucket": "arn:aws:s3:::inventory-bucket",
      "Format": "Parquet",
      "Prefix": "inventory/prod-us-bucket",
      "Encryption": {"SSEKMS": {"KeyId": "arn:aws:kms:us-east-1:1234:key/…"}}
    }
  },
  "OptionalFields": ["Size","LastModifiedDate","StorageClass","ETag","ChecksumAlgorithm"]
}
```
> 可用 `aws s3api put-bucket-inventory-configuration` 设置；多前缀可用多策略或在消费侧过滤。

##### 5.4.1.5 GCS Storage Insights（Inventory 报告，示例）
```yaml
name: insights-us-prod
bucket: prod-us-bucket
frequency: DAILY
format: PARQUET
destination_bucket: inventory-bucket
destination_path: reports/prod-us-bucket
fields: [NAME, SIZE, MD5_HASH, CRC32C, STORAGE_CLASS, UPDATED, GENERATION]
```
> 可用控制台或 `gcloud storage insights reports create` 创建。

##### 5.4.1.6 同步服务如何使用清单
1) **构建差分**：
   - 取 `main@H_prev..H_main` 的 ADDED 文件列表（来自 Iceberg/Nessie）。
   - 在**源清单**中按 `Key`（或完整 URI → `bucket`+`key`）查存在性与 `ETag/CRC32C/Size`。
   - 在**目的清单**（本地域）查目标是否已存在；若不存在或校验不一致 ⇒ 加入复制计划。
2) **避免在线罗列**：批量查询清单就能判定“需不需要复制”，无需 `LIST` 大量目录。
3) **L0 校验**：复制后再用**目的清单**抽样对比 `Size`/`ETag/CRC32C`；不一致则重试或标红。
4) **对账与审计**：以标准化清单表为基准，按 `dt`（必要时再加 `hour`）与**同步完成表**做左右连接：
   - **清单有 / 同步缺** ⇒ 该对象应存在但尚未被复制或落地，列为“缺口”。
   - **同步有 / 清单缺** ⇒ 可能是清单未新鲜或对象已被删除，标注“需复核”。
   最终输出“缺口报告”供运维与告警使用。

**示例 SQL（Athena / BigQuery）**
- 找出“ADDED 文件中在本地域仍缺失”的清单记录：
```sql
-- Athena 示例（S3 Inventory 外表）
SELECT a.key
FROM added_files a
LEFT JOIN inv_us_today b
  ON a.bucket = b.source_bucket AND a.key = b.key
WHERE b.key IS NULL;
```
- 校验大小/校验和不一致：
```sql
SELECT d.key, d.size AS expected_size, b.size AS actual_size
FROM desired_us_files d
JOIN inv_us_today b ON d.key = b.key
WHERE d.size <> b.size;
```


##### 5.4.1.7 更高频清单的利弊（Pros & Cons）

*优点（何时值得提频）*
- **降低在线 HEAD/LIST 比例与成本**：更多对象状态可由清单命中，尤其在跨区/跨云 QPS 受限或费用敏感时收益明显。
- **更快的对账与缺口定位**：缺口报告更接近实时，减少“清单未到导致误告警/误缺口”。
- **复制/校验链路更顺滑**：当 `copy_backlog_bytes` 较大且 `HEAD` 占比高时，提升清单频率可提升整体吞吐与稳定性。

*缺点（需要权衡）*
- **生成与存储成本上升**：供应商侧生成费用 + 我方存储/查询费用（更多分区/文件）。
- **处理链路更复杂**：标准化 ETL 与“完成判定”更频繁，调度/失败重试更敏感。
- **热点放大风险**：小时级清单会在固定时间点产生高并发读写与计算，需与同步窗口错峰。

*建议实践*
- **触发式提频**：默认 Daily；当满足任一条件再对“热桶/热前缀”升到 Hourly：
  - `storage_api_calls_total{op="HEAD"}` 占比高且成本超预算；
  - `regional_commit_lag_seconds` 受复制/校验阶段拖累（DB 复制正常）；
  - 缺口报告因清单不新鲜导致误报频繁。
- **错峰与配额**：将清单产出时间与同步高峰错开 10–30 分钟；为 `inventory ETL`/查询设置并发与成本配额。
- **分层策略**：仅对**活跃分区/热数据**使用高频清单；冷数据保留 Daily/Weekly。
- **监控闭环**：结合 `inventory_freshness_lag_seconds`、`storage_api_calls_total{op=HEAD}` 与成本看板，按月复盘是否需要继续维持高频率。

---

## 6. 实施与迁移

### 6.1 冷启动 (Bootstrapping)

对于一个已存在的、PB级别的大表，首次在新的地区创建副本时，不能依赖网络复制。
1.  **物理迁移**: 使用 **AWS Snowball** 或 **GCP Transfer Appliance** 等物理设备，将源存储的全量数据一次性运送到新地区的数据中心。
2.  **数据加载**: 将数据从设备加载到新地区的本地存储中。
3.  **元数据初始化**: 在完成数据加载后，执行一次特殊的“全量”元数据同步和本地化任务，在新地区的副本分支上创建第一个快照。
4.  **切换到增量**: 初始化完成后，同步服务切换到正常的、基于增量 commit 的同步模式。

> **补充说明**：当一个已有副本的地区首次开始**写入数据**时，其第一批数据文件会先写入本地存储，然后对应的提交必须通过广域网发送到主目录的 `main` 分支。由于跨地域网络延迟，这部分写入延迟会相对较高，是整体延迟模型需要考虑的一环。

> **一致性要求（Cutover）**：全量元数据同步需与增量同步对齐一个**切换点**：要么在 cutover@H 暂停新写入、完成全量快照再恢复增量；要么记录
H_cutover 并确保增量只消费 H_cutover 及之后的提交。否则会出现“全量与增量交叉覆盖/遗漏”的一致性问题。


### 6.2 安全与网络

*   **网络**: 不同云或本地机房与云之间的通信，必须通过安全的 **VPN** 或 **专线（Direct Connect / Interconnect）**。VPC/VNet 的网络ACL和安全组必须经过严格配置，只允许必要的服务端口通信。
*   **身份与权限 (IAM)**:
    *   每个同步服务都应使用一个拥有**最小权限**的 IAM 角色或服务账户。
    *   例如，美东的同步服务，其角色应只拥有“读欧洲S3”和“写美东S3”的权限，以及访问Nessie和本地目录数据库的权限。
    *   凭证应通过安全的密钥管理服务（如 AWS Secrets Manager）进行管理和轮换。

#### 6.2.1 RBAC 权限矩阵（最小权限）
| 角色 | 目录（Nessie/API） | 目录 DB | 对象存储（源） | 对象存储（本地） | 备注 |
|---|---|---|---|---|---|
| Writer 作业 | `commit` 到 `main`；只读 `getRef` | 无（经 Nessie） | 只读 | 只写本地域前缀 | 不允许触达副本库或他地域写 |
| Sync Service | 写 `main_replica_<region>`（在**主目录**上）；读 `main`/`commit log` | 只读副本库；不在副本库执行写 | 读（跨区/跨云） | 写（本地域落地前缀）+ 删除（GC 执行） | 需要 `assume role`/KMS 权限 |
| Regional Reader | 只读 `getRef`/`getCommitLog`（本地域读取端点） | 只读副本库 | 只读（本地域） | 只读（本地域） | 禁止访问写端点 |
| GC Orchestrator | 读写 `gc_candidates`（主库） | 主库可写 | 只读 | （可选）删除主目录位置 | 通常位于主目录地域 |

---

## 7. 附录：与初始设计的对比分析

### 7.1 核心思想演进

我们的讨论始于一个**主从复制（Primary-Replica）**模型，其目标是在云端创建一个高可靠的**只读镜像**，这是一个典型的**灾难恢复（Disaster Recovery）**方案。

经过层层深入的探讨，我们最终演进到了一个**对等网络（Peer-to-Peer）**模型，其目标是支持**全球化的读写**，这是一个真正的**高可用（High Availability）**架构。

### 7.2 详细差异对比

| 特性 / 组件 | 初始设计 (`iceberg-arch-hybrid-replica-dr.md`) | 演进后的设计 (`iceberg-arch-geo-distributed-ha.md`) | 差异与演进原因 |
| :--- | :--- | :--- | :--- |
| **核心架构** | 主从复制 (本地写，云端读) | 地理分布式 (全球任意地点可读写) | **目标升级**: 从简单的灾难恢复，升级为支持全球化的业务需求。 |
| **写入能力** | **中心化**: 只有本地的 SoT (单一真相源) 能接受写入。 | **去中心化**: 任何地点的任务都可以写入其本地存储。 | 为了给全球各地的写入任务提供最低的延迟。 |
| **目录服务** | 本地单一数据库 (Postgres) + 云端只读目录 (BLMS)。 | **全局事务性目录** (Nessie + 分布式数据库)。 | 消除写入路径的单点故障，为全球写入提供一个统一、高可用的元数据真相源。 |
| **高可用性** | **读可用**: 是 (云端副本可用)。<br>**写可用**: **否** (本地 SoT 是单点故障)。 | **读写均高可用**: **是** (采用主从异步复制的目录后端，主集群故障时可手动切换)。 | 满足了真正的业务连续性要求，而不仅仅是数据备份。 |
| **数据同步模型** | **推送模型 (Push)**: 一个中心的 `Replicator` 将数据从本地**推送**到云端。 | **拉取模型 (Pull)**: 每个地区的 `Sync Service` 从其他地区**拉取**数据。 | 数据的来源不再是单一地点，因此每个节点都必须主动去同步其他节点的数据。 |
| **同步协调机制** | **显式协调**: 使用 `_inprogress` 和 `_ready` marker 文件来作为信号，通知下游的 `Follower` 服务。 | **隐式协调**: **不再需要 Marker 文件**。Nessie 的 commit 本身就是全局的、原子性的信号。 | 协调机制由应用层的、复杂的 Marker 文件逻辑，演进为目录服务内置的、更可靠的事务日志。 |
| **元数据处理** | 复制器物理复制元数据文件；Follower 更新一个独立的云端目录。 | 同步服务在拉取完数据后，生成一份**路径本地化**的元数据，并提交到 **Nessie 的副本分支**上。 | 为本地查询引擎提供一个干净、高效的读取视图，避免了引擎层改造的复杂性。 |
| **读取路径** | 云端引擎查询一个独立的、只读的目录副本 (BLMS)。 | 各地引擎查询同一个全局目录 (Nessie) 的**不同分支** (例如 `main_replica_us`)。 | 简化了系统的拓扑结构，利用了 Nessie 强大的分支能力来隔离读写。 |
| **架构复杂度** | 概念简单，但有多个独立的移动部件 (Replicator, Follower, Marker)，协调逻辑复杂。 | 核心概念更先进 (Raft, Nessie)，但一旦建立，系统运行时的移动部件更少，逻辑更内聚、更健壮。 | 将复杂性从“过程协调”转移到了“状态共识”，后者有更成熟的理论和工具支持。 |

### 7.3 总结

总而言之，我们的设计从一个相对传统的、用于数据备份和灾难恢复的**主从架构**，演进成了一个采用现代分布式系统核心思想（如共识协议、控制与数据平面分离）的、真正意义上的**全球化、高可用架构**。

它成功地将一个复杂的海量异构数据复制问题，分解为了一个已解决的元数据共识问题（区域分支）和一个可并行化的数据搬运问题（存储副本）。

### 7.4 同步 DAG 伪代码示例

下图展示了一个最小可运行的同步 DAG 任务划分，用于指导开发者实现端到端流程。

```python
with DAG("iceberg_sync_us", schedule_interval="*/5 * * * *") as dag:

    # 1. 列出差异
    t1_list_added_files = BashOperator(
        task_id="list_added_files",
        bash_command="python tasks/list_added_files.py --from H_prev --to H_main"
    )

    # 2. 生成复制计划
    t2_plan_copy = BashOperator(
        task_id="plan_copy_batches",
        bash_command="python tasks/plan_copy.py --input added_files.json --output plan.json"
    )

    # 3. 并行复制任务
    t3_copy_batches = PythonOperator(
        task_id="copy_batches",
        python_callable=copy_batches,
        op_kwargs={"plan_file": "plan.json"}
    )

    # 4. 校验
    t4_verify = PythonOperator(
        task_id="verify_L0",
        python_callable=verify_files,
        op_kwargs={"plan_file": "plan.json"}
    )

    # 5. 元数据本地化
    t5_localize = PythonOperator(
        task_id="localize_metadata",
        python_callable=localize_metadata,
        op_kwargs={"main_hash": "{{ ti.xcom_pull('list_added_files') }}"} 
    )

    # 6. CAS 提交区域分支
    t6_commit = PythonOperator(
        task_id="cas_commit_replica",
        python_callable=cas_commit,
        op_kwargs={"region": "us"}
    )

    # 7. 指标上报
    t7_metrics = PythonOperator(
        task_id="emit_metrics",
        python_callable=emit_metrics
    )

    # 注：若是该地域的**首次写入**，提交会通过广域网转发到主目录 main，延迟相对更高（见 6.1 冷启动补充说明）

    # 定义依赖
    t1_list_added_files >> t2_plan_copy >> t3_copy_batches >> t4_verify >> t5_localize >> t6_commit >> t7_metrics
```
> 注意：以上为伪代码，仅展示任务边界与依赖关系。实际实现需结合 Iceberg API、Nessie API 与 Rclone CLI。