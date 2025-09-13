package com.streamfirst.iceberg.hybrid.adapters

import com.streamfirst.iceberg.hybrid.domain.DomainError.{ConfigurationError, StorageError}
import com.streamfirst.iceberg.hybrid.domain.{BatchRegistrationResult, Region, StorageLocation, TableId}
import com.streamfirst.iceberg.hybrid.ports.{RegionStatus, RegistryPort}
import zio.{IO, Ref, ZIO, ZLayer}

/** In-memory implementation of RegistryPort for testing and development. NOT suitable for
  * production use - data is lost on restart. Uses ZIO Ref for thread-safe atomic operations.
  */
final case class InMemoryRegistryAdapter(
    tableLocations: Ref[Map[(TableId, Region), String]],
    regionStorage: Ref[Map[Region, StorageLocation]],
    regionStatuses: Ref[Map[Region, RegionStatus]]
) extends RegistryPort:

  override def getTableDataPath(
      tableId: TableId,
      region: Region
  ): IO[StorageError, Option[String]] = tableLocations.get.map(_.get((tableId, region)))

  override def registerTableLocation(
      tableId: TableId,
      region: Region,
      dataPath: String
  ): IO[StorageError, Unit] =
    tableLocations.update(_.updated((tableId, region), dataPath)) *>
      ZIO.logDebug(s"Registered table location: $tableId in region ${region.id} at $dataPath")

  override def getTableRegions(tableId: TableId): IO[StorageError, List[Region]] =
    tableLocations.get.map { locations => locations.keys.filter(_._1 == tableId).map(_._2).toList }

  override def getActiveRegions: IO[ConfigurationError, List[Region]] =
    regionStatuses.get.map { statuses =>
      statuses.filter(_._2 == RegionStatus.Active).keys.toList.distinct
    }

  override def registerRegion(
      region: Region,
      storageLocation: StorageLocation
  ): IO[ConfigurationError, Unit] =
    for
      _ <- regionStorage.update(_ + (region -> storageLocation))
      _ <- regionStatuses.update(_ + (region -> RegionStatus.Active))
      _ <- ZIO.logInfo(s"Registered region ${region.id} with storage")
    yield ()

  override def getRegionStorage(region: Region): IO[StorageError, Option[StorageLocation]] =
    regionStorage.get.map(_.get(region))

  override def updateRegionStatus(
      region: Region,
      status: RegionStatus
  ): IO[ConfigurationError, Unit] =
    regionStatuses.update(_ + (region -> status)) *>
      ZIO.logInfo(s"Updated region ${region.id} status to $status")

  override def getRegionTables(region: Region): IO[StorageError, List[TableId]] =
    tableLocations.get.map { locations => locations.keys.filter(_._2 == region).map(_._1).toList }

  override def registerTableLocationsBatch(
      registrations: List[(TableId, Region, String)]
  ): IO[StorageError, BatchRegistrationResult] =
    ZIO.attempt {
      val updates = registrations.map { case (tableId, region, path) => 
        (tableId, region) -> path
      }.toMap
      BatchRegistrationResult.success(registrations.size)
    }.flatMap { result =>
      tableLocations.update { current =>
        registrations.foldLeft(current) { case (acc, (tableId, region, path)) =>
          acc.updated((tableId, region), path)
        }
      }.as(result)
    }.orElse(ZIO.succeed(BatchRegistrationResult(registrations.size, 0, registrations.size)))

  override def getTableDataPathsBatch(
      requests: List[(TableId, Region)]
  ): IO[StorageError, Map[(TableId, Region), Option[String]]] =
    tableLocations.get.map { locations =>
      requests.map { request =>
        request -> locations.get(request)
      }.toMap
    }

  override def registerTableInRegionsBatch(
      tableId: TableId,
      regionPaths: List[(Region, String)]
  ): IO[StorageError, BatchRegistrationResult] =
    ZIO.attempt {
      BatchRegistrationResult.success(regionPaths.size)
    }.flatMap { result =>
      tableLocations.update { current =>
        regionPaths.foldLeft(current) { case (acc, (region, path)) =>
          acc.updated((tableId, region), path)
        }
      }.as(result)
    }.orElse(ZIO.succeed(BatchRegistrationResult(regionPaths.size, 0, regionPaths.size)))

object InMemoryRegistryAdapter:
  def live: ZLayer[Any, Nothing, RegistryPort] =
    ZLayer.fromZIO {
      for
        tableLocations <- Ref.make(Map.empty[(TableId, Region), String])
        regionStorage <- Ref.make(Map.empty[Region, StorageLocation])
        regionStatuses <- Ref.make(Map.empty[Region, RegionStatus])
      yield InMemoryRegistryAdapter(tableLocations, regionStorage, regionStatuses)
    }

  def withSampleData: ZLayer[Any, Nothing, RegistryPort] =
    ZLayer.fromZIO {
      for
        tableLocations <- Ref.make(Map.empty[(TableId, Region), String])
        regionStorage <- Ref.make(Map.empty[Region, StorageLocation])
        regionStatuses <- Ref.make(Map.empty[Region, RegionStatus])
        adapter = InMemoryRegistryAdapter(tableLocations, regionStorage, regionStatuses)

        // Add sample data using atomic operations
        usEast1Storage = StorageLocation
          .s3(Region.UsEast1, "iceberg-data-us-east-1", Some("tables"))
        euWest1Storage = StorageLocation
          .s3(Region.EuWest1, "iceberg-data-eu-west-1", Some("tables"))

        _ <- regionStorage
          .update(_ ++ Map(Region.UsEast1 -> usEast1Storage, Region.EuWest1 -> euWest1Storage))
        _ <- regionStatuses.update(
          _ ++ Map(Region.UsEast1 -> RegionStatus.Active, Region.EuWest1 -> RegionStatus.Active)
        )

        // Register sample table locations
        sampleTable = TableId("analytics", "user_events")
        samplePath = "tables/analytics/user_events"
        _ <- tableLocations.update(
          _ ++ Map(
            (sampleTable, Region.UsEast1) -> samplePath,
            (sampleTable, Region.EuWest1) -> samplePath
          )
        )

        _ <- ZIO.logInfo("Created registry adapter with sample data")
      yield adapter
    }
