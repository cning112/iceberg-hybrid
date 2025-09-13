package com.streamfirst.iceberg.hybrid.domain

import java.time.Instant

/** Represents the status of a write coordination job */
enum WriteJobStatus:
  case Pending
  case RequestingApproval
  case Approved  
  case CommittingLocal
  case SynchronizingRegions
  case Completed
  case Failed(error: String)

/** Represents a write coordination job */
case class WriteJob(
  writeJobId: WriteJobId,
  tableId: TableId,
  sourceRegion: Region,
  commitId: Option[CommitId] = None,
  status: WriteJobStatus,
  targetRegions: List[Region] = Nil,
  createdAt: Instant,
  updatedAt: Instant,
  completedAt: Option[Instant] = None
):
  def withStatus(newStatus: WriteJobStatus): WriteJob = 
    val now = Instant.now()
    val completed = newStatus match
      case WriteJobStatus.Completed | WriteJobStatus.Failed(_) => Some(now)
      case _ => completedAt
    
    this.copy(
      status = newStatus, 
      updatedAt = now,
      completedAt = completed
    )
    
  def withCommitId(newCommitId: CommitId): WriteJob =
    this.copy(commitId = Some(newCommitId), updatedAt = Instant.now())
    
  def withTargetRegions(regions: List[Region]): WriteJob =
    this.copy(targetRegions = regions, updatedAt = Instant.now())

object WriteJob:
  def create(
    tableId: TableId,
    sourceRegion: Region
  ): WriteJob =
    val now = Instant.now()
    WriteJob(
      writeJobId = WriteJobId.generate(),
      tableId = tableId,
      sourceRegion = sourceRegion,
      status = WriteJobStatus.Pending,
      createdAt = now,
      updatedAt = now
    )