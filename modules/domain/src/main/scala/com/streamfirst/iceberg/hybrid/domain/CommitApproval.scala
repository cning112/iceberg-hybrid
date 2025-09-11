package com.streamfirst.iceberg.hybrid.domain

import java.time.Instant

/** Represents approval for a commit request from the global commit gate. Once approved, the commit
  * can proceed and sync events will be created.
  */
final case class CommitApproval(
  requestId: String,
  tableId: TableId,
  commitId: CommitId,
  approvedAt: Instant,
  approvedRegions: List[Region]
)

object CommitApproval:
  def approve(
    request: CommitRequest,
    regions: List[Region]
  ): CommitApproval =
    CommitApproval(
      requestId = request.requestId,
      tableId = request.tableId,
      commitId = request.metadata.commitId,
      approvedAt = Instant.now(),
      approvedRegions = regions
    )
