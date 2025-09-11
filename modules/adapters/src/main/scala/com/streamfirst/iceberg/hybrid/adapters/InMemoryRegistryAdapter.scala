package com.streamfirst.iceberg.hybrid.adapters

import com.streamfirst.iceberg.hybrid.domain.*
import com.streamfirst.iceberg.hybrid.domain.DomainError.*
import com.streamfirst.iceberg.hybrid.ports.{RegionStatus, RegistryPort}
import zio.logging.*

import scala.collection.concurrent.TrieMap

/** In-memory implementation of RegistryPort for testing and development. NOT suitable for
  * production use - data is lost on restart.
  */
final case class InMemoryRegistryAdapter() extends RegistryPort:
  val tableLocations = TrieMap[(TableId, Region), String]()
  val regionStorage = TrieMap[Region, StorageLocation]()
  val regionStatuses = TrieMap[Region, RegionStatus]()

  override def getTableDataPath(
    tableId: TableId,
    region: Region): IO[StorageError, Option[String]] =
    ZIO.succeed {
      tableLocations.get((tableId, region))
    }

  override def registerTableLocation(
    tableId: TableId,
    region: Region,
    dataPath: String): IO[StorageError, Unit] =
    ZIO
      .succeed {
        tableLocations.put((tableId, region), dataPath)
        ()
      }
      .tap(_ =>
        ZIO.logDebug(s"Registered table location: $tableId in region ${region.id} at $dataPath"))

  override def getTableRegions(tableId: TableId): IO[StorageError, List[Region]] =
    ZIO.succeed {
      tableLocations.keys
        .filter(_._1 == tableId)
        .map(_._2)
        .toList
    }

  override def getActiveRegions(): IO[ConfigurationError, List[Region]] =
    ZIO.succeed {
      regionStatuses
        .filter(_._2 == RegionStatus.Active)
        .keys
        .toList
    }

  override def registerRegion(
    region: Region,
    storageLocation: StorageLocation): IO[ConfigurationError, Unit] =
    ZIO
      .succeed {
        regionStorage.put(region, storageLocation)
        regionStatuses.put(region, RegionStatus.Active)
        ()
      }
      .tap(_ => ZIO.logInfo(s"Registered region ${region.id} with storage"))

  override def getRegionStorage(region: Region): IO[StorageError, Option[StorageLocation]] =
    ZIO.succeed {
      regionStorage.get(region)
    }

  override def updateRegionStatus(
    region: Region,
    status: RegionStatus): IO[ConfigurationError, Unit] =
    ZIO
      .succeed {
        regionStatuses.put(region, status)
        ()
      }
      .tap(_ => ZIO.logInfo(s"Updated region ${region.id} status to $status"))

  override def getRegionTables(region: Region): IO[StorageError, List[TableId]] =
    ZIO.succeed {
      tableLocations.keys
        .filter(_._2 == region)
        .map(_._1)
        .toList
    }

object InMemoryRegistryAdapter:
  def live: ZLayer[Any, Nothing, RegistryPort] =
    ZLayer.succeed(InMemoryRegistryAdapter())

  def withSampleData: ZLayer[Any, Nothing, RegistryPort] =
    ZLayer.fromZIO {
      val adapter = InMemoryRegistryAdapter()

      // Add some sample regions synchronously (for simplicity in test setup)
      val usEast1Storage =
        StorageLocation.s3(Region.UsEast1, "iceberg-data-us-east-1", Some("tables"))
      val euWest1Storage =
        StorageLocation.s3(Region.EuWest1, "iceberg-data-eu-west-1", Some("tables"))

      adapter.regionStorage.put(Region.UsEast1, usEast1Storage)
      adapter.regionStorage.put(Region.EuWest1, euWest1Storage)
      adapter.regionStatuses.put(Region.UsEast1, RegionStatus.Active)
      adapter.regionStatuses.put(Region.EuWest1, RegionStatus.Active)

      // Register some sample table locations
      val sampleTable = TableId("analytics", "user_events")
      adapter.tableLocations.put((sampleTable, Region.UsEast1), "tables/analytics/user_events")
      adapter.tableLocations.put((sampleTable, Region.EuWest1), "tables/analytics/user_events")

      ZIO.succeed(adapter).tap(_ => ZIO.logInfo("Created registry adapter with sample data"))
    }
