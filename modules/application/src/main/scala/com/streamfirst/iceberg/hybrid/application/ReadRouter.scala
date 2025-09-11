package com.streamfirst.iceberg.hybrid.application

import com.streamfirst.iceberg.hybrid.domain.*
import com.streamfirst.iceberg.hybrid.domain.DomainError.*
import com.streamfirst.iceberg.hybrid.ports.*
import zio.logging.*

/** Routes read operations to the optimal region based on data locality and availability. Implements
  * intelligent routing for geo-distributed read queries.
  */
final case class ReadRouter(
  catalogPort: CatalogPort,
  registryPort: RegistryPort,
  storagePort: StoragePort
):
  /** Routes a read request to the best available region for the specified table.
    */
  def routeRead(
    tableId: TableId,
    preferredRegion: Option[Region] = None
  ): IO[CatalogError | StorageError, ReadLocation] =
    for
      _ <- ZIO.logDebug(s"Routing read request for table $tableId")

      // Get all regions where this table has data
      availableRegions <- registryPort
        .getTableRegions(tableId)
        .mapError(storageErrorToCatalogError)

      _ <- ZIO.when(availableRegions.isEmpty)(
        ZIO.fail(DomainError.TableNotFound(tableId))
      )

      // Choose the best region based on preference and availability
      bestRegion <- chooseBestRegion(availableRegions, preferredRegion)

      // Get storage location for the chosen region
      storageLocation <- storagePort
        .getStorageLocation(bestRegion)
        .mapError(identity)

      // Get table data path in that region
      dataPath <- registryPort
        .getTableDataPath(tableId, bestRegion)
        .mapError(storageErrorToCatalogError)
        .someOrFail(DomainError.TableNotFound(tableId))

      _ <- ZIO.logInfo(s"Routed read for table $tableId to region ${bestRegion.id}")
    yield ReadLocation(tableId, bestRegion, storageLocation, dataPath)

  /** Chooses the best region for reading based on availability and preference.
    */
  private def chooseBestRegion(
    availableRegions: List[Region],
    preferredRegion: Option[Region]
  ): IO[CatalogError, Region] =
    preferredRegion match
      case Some(preferred) if availableRegions.contains(preferred) =>
        ZIO.succeed(preferred)
      case _ =>
        // For now, just pick the first available region
        // TODO: Implement more sophisticated routing based on latency, load, etc.
        ZIO
          .fromOption(availableRegions.headOption)
          .orElseFail(DomainError.CatalogUnavailable("No regions available"))

  /** Gets the latest metadata for a table, preferring local region if available.
    */
  def getLatestMetadata(
    tableId: TableId,
    preferredRegion: Option[Region] = None
  ): IO[CatalogError, TableMetadata] =
    for
      metadataOpt <- catalogPort.getLatestMetadata(tableId)
      metadata <- ZIO
        .fromOption(metadataOpt)
        .orElseFail(DomainError.TableNotFound(tableId))
    yield metadata

  /** Gets metadata for a specific commit, with region preference.
    */
  def getMetadata(
    tableId: TableId,
    commitId: CommitId,
    preferredRegion: Option[Region] = None
  ): IO[CatalogError, TableMetadata] =
    for
      metadataOpt <- catalogPort.getMetadata(tableId, commitId)
      metadata <- ZIO
        .fromOption(metadataOpt)
        .orElseFail(DomainError.CommitNotFound(tableId, commitId))
    yield metadata

  private def storageErrorToCatalogError(error: StorageError): CatalogError =
    error match
      case DomainError.StorageLocationNotFound(region) =>
        DomainError.CatalogUnavailable(s"Storage not found for region ${region.id}")
      case _ =>
        DomainError.CatalogUnavailable(error.message)

/** Represents the chosen location for reading table data.
  */
final case class ReadLocation(
  tableId: TableId,
  region: Region,
  storageLocation: StorageLocation,
  dataPath: String
)

object ReadRouter:
  val live: ZLayer[CatalogPort & RegistryPort & StoragePort, Nothing, ReadRouter] =
    ZLayer {
      for
        catalogPort <- ZIO.service[CatalogPort]
        registryPort <- ZIO.service[RegistryPort]
        storagePort <- ZIO.service[StoragePort]
      yield ReadRouter(catalogPort, registryPort, storagePort)
    }
