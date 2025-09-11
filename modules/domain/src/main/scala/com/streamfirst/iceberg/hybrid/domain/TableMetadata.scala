package com.streamfirst.iceberg.hybrid.domain

import java.time.Instant

/** Represents a specific version of Apache Iceberg table metadata. Contains all information needed
  * to describe the table's structure and data files at a particular point in time. Metadata is
  * versioned by commit ID and replicated across regions for geo-distributed access.
  */
final case class TableMetadata(
  tableId: TableId,
  commitId: CommitId,
  sourceRegion: Region,
  timestamp: Instant,
  dataFiles: List[StoragePath],
  schema: String
):
  override def toString: String =
    s"TableMetadata(tableId=$tableId, commitId=$commitId, sourceRegion=${sourceRegion.id}, " +
      s"timestamp=$timestamp, dataFileCount=${dataFiles.size})"

object TableMetadata:
  def create(
    tableId: TableId,
    sourceRegion: Region,
    dataFiles: List[StoragePath],
    schema: String
  ): TableMetadata =
    TableMetadata(
      tableId = tableId,
      commitId = CommitId.generate(),
      sourceRegion = sourceRegion,
      timestamp = Instant.now(),
      dataFiles = dataFiles,
      schema = schema
    )
