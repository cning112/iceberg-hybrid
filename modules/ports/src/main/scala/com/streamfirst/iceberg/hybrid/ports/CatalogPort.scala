package com.streamfirst.iceberg.hybrid.ports

import com.streamfirst.iceberg.hybrid.domain.*
import com.streamfirst.iceberg.hybrid.domain.DomainError.CatalogError
import zio.stream.ZStream
import zio.{IO, ZIO}

/** Port for Apache Iceberg catalog operations. Abstracts the underlying catalog implementation
  * (Nessie, Hive, Hadoop, etc.) using ZIO effects for type-safe operations.
  */
trait CatalogPort:
  /** Gets table metadata for a specific commit/version. */
  def getMetadata(tableId: TableId, commitId: CommitId): IO[CatalogError, Option[TableMetadata]]

  /** Gets the latest metadata for a table. */
  def getLatestMetadata(tableId: TableId): IO[CatalogError, Option[TableMetadata]]

  /** Commits new metadata for a table, creating a new version. */
  def commitMetadata(metadata: TableMetadata): IO[CatalogError, CommitId]

  /** Lists all tables in a namespace. Use with caution for large namespaces. */
  def listTables(namespace: String): IO[CatalogError, List[TableId]]

  /** Lists tables in a namespace with pagination support. Preferred for large namespaces. */
  def listTablesPaginated(
      namespace: String,
      pagination: PaginationRequest
  ): IO[CatalogError, PaginatedResult[TableId]]

  /** Streams all tables in a namespace. Preferred for very large namespaces. */
  def listTablesStream(namespace: String): ZStream[Any, CatalogError, TableId]

  /** Checks if a table exists in the catalog. */
  def tableExists(tableId: TableId): IO[CatalogError, Boolean]

  /** Creates a new table with the specified metadata. */
  def createTable(metadata: TableMetadata): IO[CatalogError, Unit]

  /** Drops a table from the catalog. */
  def dropTable(tableId: TableId): IO[CatalogError, Unit]

  /** Gets all commit history for a table. Use with caution for tables with long history. */
  def getCommitHistory(tableId: TableId): IO[CatalogError, List[CommitId]]

  /** Gets commit history for a table with pagination support. Preferred for tables with long history. */
  def getCommitHistoryPaginated(
      tableId: TableId,
      pagination: PaginationRequest
  ): IO[CatalogError, PaginatedResult[CommitId]]

  /** Streams commit history for a table. Preferred for very long histories. */
  def getCommitHistoryStream(tableId: TableId): ZStream[Any, CatalogError, CommitId]

  /** Gets metadata for multiple commits in parallel. */
  def getMetadataBatch(
      requests: List[(TableId, CommitId)]
  ): IO[CatalogError, Map[(TableId, CommitId), TableMetadata]]

object CatalogPort:
  /** ZIO service accessors for dependency injection */
  def getMetadata(
      tableId: TableId,
      commitId: CommitId
  ): ZIO[CatalogPort, CatalogError, Option[TableMetadata]] =
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

  def dropTable(tableId: TableId): ZIO[CatalogPort, CatalogError, Unit] =
    ZIO.serviceWithZIO[CatalogPort](_.dropTable(tableId))

  def getCommitHistory(tableId: TableId): ZIO[CatalogPort, CatalogError, List[CommitId]] =
    ZIO.serviceWithZIO[CatalogPort](_.getCommitHistory(tableId))

  def getMetadataBatch(
      requests: List[(TableId, CommitId)]
  ): ZIO[CatalogPort, CatalogError, Map[(TableId, CommitId), TableMetadata]] =
    ZIO.serviceWithZIO[CatalogPort](_.getMetadataBatch(requests))

  def listTablesPaginated(
      namespace: String,
      pagination: PaginationRequest
  ): ZIO[CatalogPort, CatalogError, PaginatedResult[TableId]] =
    ZIO.serviceWithZIO[CatalogPort](_.listTablesPaginated(namespace, pagination))

  def listTablesStream(namespace: String): ZStream[CatalogPort, CatalogError, TableId] =
    ZStream.serviceWithStream[CatalogPort](_.listTablesStream(namespace))

  def getCommitHistoryPaginated(
      tableId: TableId,
      pagination: PaginationRequest
  ): ZIO[CatalogPort, CatalogError, PaginatedResult[CommitId]] =
    ZIO.serviceWithZIO[CatalogPort](_.getCommitHistoryPaginated(tableId, pagination))

  def getCommitHistoryStream(tableId: TableId): ZStream[CatalogPort, CatalogError, CommitId] =
    ZStream.serviceWithStream[CatalogPort](_.getCommitHistoryStream(tableId))
