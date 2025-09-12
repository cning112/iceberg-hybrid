package com.streamfirst.iceberg.hybrid.domain

import java.time.Instant

/** Represents a request to commit changes to a table. This is processed by the commit gate to
  * ensure consistency across regions before allowing the commit to proceed.
  */
final case class CommitRequest(
    tableId: TableId,
    requestId: String,
    sourceRegion: Region,
    metadata: TableMetadata,
    timestamp: Instant
)

object CommitRequest:
  def create(tableId: TableId, sourceRegion: Region, metadata: TableMetadata): CommitRequest =
    CommitRequest(
      tableId = tableId,
      requestId = EventId.generate("commit-req").value,
      sourceRegion = sourceRegion,
      metadata = metadata,
      timestamp = Instant.now()
    )
