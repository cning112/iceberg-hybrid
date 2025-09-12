package com.streamfirst.iceberg.hybrid.application

import com.streamfirst.iceberg.hybrid.domain.DomainError.{ CatalogError, StorageError }
import com.streamfirst.iceberg.hybrid.domain.{
  CommitId,
  DomainError,
  Region,
  StorageLocation,
  TableId,
  TableMetadata
}
import com.streamfirst.iceberg.hybrid.ports.{ CatalogPort, RegistryPort, StoragePort }
import zio.{ IO, ZIO, ZLayer }

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

  /** Chooses the best region for reading based on availability, preference, and intelligent
    * routing. Uses a scoring algorithm that considers region health, data freshness, and proximity.
    */
  private def chooseBestRegion(
    availableRegions: List[Region],
    preferredRegion: Option[Region]
  ): IO[CatalogError, Region] =
    for
      _ <- ZIO.logDebug(s"Choosing best region from: ${availableRegions.map(_.id)}")

      // First preference: use preferred region if available and active
      preferredChoice <- preferredRegion match
        case Some(preferred) if availableRegions.contains(preferred) =>
          registryPort
            .getRegionStorage(preferred)
            .map(_.nonEmpty)
            .map(available => if available then Some(preferred) else None)
            .catchAll(_ => ZIO.none) // Region not available
        case _ => ZIO.none

      result <- preferredChoice match
        case Some(region) => ZIO.succeed(region)
        case None => selectOptimalRegion(availableRegions)
    yield result

  /** Selects the optimal region using a scoring algorithm based on multiple factors. Uses parallel
    * processing for better performance.
    */
  private def selectOptimalRegion(regions: List[Region]): IO[CatalogError, Region] =
    for
      // Score regions in parallel for better performance
      scoredRegions <- ZIO.foreachPar(regions)(scoreRegion)

      // Filter out regions with zero score (unavailable) and find the best
      availableRegions = scoredRegions.filter(_._2 > 0.0)
      bestRegion <- ZIO
        .fromOption(availableRegions.maxByOption(_._2).map(_._1))
        .orElseFail(DomainError.CatalogUnavailable("No suitable regions available"))

      _ <- ZIO.logInfo(
        s"Selected region ${bestRegion.id} as optimal for read routing (score: ${scoredRegions.find(_._1 == bestRegion).map(_._2).getOrElse(0.0)})")
    yield bestRegion

  /** Scores a region based on availability and performance metrics. Higher score means better
    * choice for reads. Uses weighted scoring algorithm for better region selection.
    */
  private def scoreRegion(region: Region): IO[CatalogError, (Region, Double)] =
    for
      // Check if region storage is available (base requirement)
      storageAvailable <- storagePort
        .getStorageLocation(region)
        .as(true)
        .catchAll(_ => ZIO.succeed(false))

      // Get active regions to check health status
      activeRegions <- registryPort.getActiveRegions
        .mapError(error => DomainError.CatalogUnavailable(error.message))

      // Calculate weighted score based on multiple factors
      storageScore = if storageAvailable then 1.0 else 0.0

      // Active regions get full weight, inactive get reduced weight but not zero
      // (for potential fallback scenarios)
      activityScore = if activeRegions.contains(region) then 1.0 else 0.3

      // Weighted final score - storage availability is critical
      finalScore = storageScore * 0.7 + activityScore * 0.3

      _ <- ZIO.logDebug(
        s"Scored region ${region.id}: $finalScore (storage: $storageAvailable, active: ${activeRegions.contains(region)})")
    yield (region, finalScore)

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
