package com.streamfirst.iceberg.hybrid.ports

import com.streamfirst.iceberg.hybrid.domain.*
import com.streamfirst.iceberg.hybrid.domain.DomainError.CatalogError
import zio.{IO, ZIO}

/** Port for Apache Iceberg catalog operations. Abstracts the underlying catalog implementation
  * (Nessie, Hive, Hadoop, etc.) using ZIO effects for type-safe operations.
  */
trait CatalogPort:
  /** Gets table metadata for a specific commit/version.
    */
  def getMetadata(tableId: TableId, commitId: CommitId): IO[CatalogError, Option[TableMetadata]]

  /** Gets the latest metadata for a table.
    */
  def getLatestMetadata(tableId: TableId): IO[CatalogError, Option[TableMetadata]]

  /** Commits new metadata for a table, creating a new version.
    */
  def commitMetadata(metadata: TableMetadata): IO[CatalogError, CommitId]

  /** Lists all tables in a namespace.
    */
  def listTables(namespace: String): IO[CatalogError, List[TableId]]

  /** Checks if a table exists in the catalog.
    */
  def tableExists(tableId: TableId): IO[CatalogError, Boolean]

  /** Creates a new table with the specified metadata.
    */
  def createTable(metadata: TableMetadata): IO[CatalogError, Unit]

  /** Drops a table from the catalog.
    */
  def dropTable(tableId: TableId): IO[CatalogError, Unit]

  /** Gets all commit history for a table.
    */
  def getCommitHistory(tableId: TableId): IO[CatalogError, List[CommitId]]

  /** Gets metadata for multiple commits in parallel.
    */
  def getMetadataBatch(
    requests: List[(TableId, CommitId)]): IO[CatalogError, Map[(TableId, CommitId), TableMetadata]]

object CatalogPort:
  /** ZIO service accessors for dependency injection
    */
  def getMetadata(
    tableId: TableId,
    commitId: CommitId): ZIO[CatalogPort, CatalogError, Option[TableMetadata]] =
    ZIO.serviceWithZIO[CatalogPort](_.getMetadata(tableId, commitId))

  def getLatestMetadata(tableId: TableId): ZIO[CatalogPort, CatalogError, Option[TableMetadata]] =
    ZIO.serviceWithZIO[CatalogPort](_.getLatestMetadata(tableId))

  def commitMetadata(metadata: TableMetadata): ZIO[CatalogPort, CatalogError, CommitId] =
    ZIO.serviceWithZIO[CatalogPort](_.commitMetadata(metadata))

  def listTables(namespace: String): ZIO[CatalogPort, CatalogError, List[TableId]] =
    ZIO.serviceWithZIO[CatalogPort](_.listTables(namespace))

  def tableExists(tableId: TableId): ZIO[CatalogPort, CatalogError, Boolean] =
    ZIO.serviceWithZIO[CatalogPort](_.tableExists(tableId))

  def createTable(metadata: TableMetadata): ZIO[CatalogPort, CatalogError, Unit] =
    ZIO.serviceWithZIO[CatalogPort](_.createTable(metadata))
