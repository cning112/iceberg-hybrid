package com.streamfirst.iceberg.hybrid.ports

import com.streamfirst.iceberg.hybrid.domain.DomainError.{ConfigurationError, StorageError}
import com.streamfirst.iceberg.hybrid.domain.{BatchRegistrationResult, Region, StorageLocation, TableId}
import zio.{IO, ZIO}

/** Port for managing the global registry of storage locations and regional configurations. Tracks
  * where table data is stored across different regions.
  */
trait RegistryPort:
  /** Gets the storage path for a table in a specific region. */
  def getTableDataPath(tableId: TableId, region: Region): IO[StorageError, Option[String]]

  /** Registers a storage location for a table in a region. */
  def registerTableLocation(
      tableId: TableId,
      region: Region,
      dataPath: String
  ): IO[StorageError, Unit]

  /** Gets all regions where a table has data stored. */
  def getTableRegions(tableId: TableId): IO[StorageError, List[Region]]

  /** Gets all active regions in the system. */
  def getActiveRegions: IO[ConfigurationError, List[Region]]

  /** Registers a new region in the system. */
  def registerRegion(region: Region, storageLocation: StorageLocation): IO[ConfigurationError, Unit]

  /** Gets the storage configuration for a specific region. */
  def getRegionStorage(region: Region): IO[StorageError, Option[StorageLocation]]

  /** Updates the status of a region (active, inactive, maintenance). */
  def updateRegionStatus(region: Region, status: RegionStatus): IO[ConfigurationError, Unit]

  /** Gets all tables that have data in a specific region. */
  def getRegionTables(region: Region): IO[StorageError, List[TableId]]

  /** Registers multiple table locations in batch for better performance. */
  def registerTableLocationsBatch(
      registrations: List[(TableId, Region, String)]
  ): IO[StorageError, BatchRegistrationResult]

  /** Gets table data paths for multiple tables in batch. */
  def getTableDataPathsBatch(
      requests: List[(TableId, Region)]
  ): IO[StorageError, Map[(TableId, Region), Option[String]]]

  /** Registers a table across multiple regions in batch. */
  def registerTableInRegionsBatch(
      tableId: TableId,
      regionPaths: List[(Region, String)]
  ): IO[StorageError, BatchRegistrationResult]

enum RegionStatus:
  case Active
  case Inactive
  case Maintenance
  case Failed

object RegistryPort:
  /** ZIO service accessors for dependency injection */
  def getTableDataPath(
      tableId: TableId,
      region: Region
  ): ZIO[RegistryPort, StorageError, Option[String]] =
    ZIO.serviceWithZIO[RegistryPort](_.getTableDataPath(tableId, region))

  def registerTableLocation(
      tableId: TableId,
      region: Region,
      dataPath: String
  ): ZIO[RegistryPort, StorageError, Unit] =
    ZIO.serviceWithZIO[RegistryPort](_.registerTableLocation(tableId, region, dataPath))

  def getTableRegions(tableId: TableId): ZIO[RegistryPort, StorageError, List[Region]] =
    ZIO.serviceWithZIO[RegistryPort](_.getTableRegions(tableId))

  def getActiveRegions: ZIO[RegistryPort, ConfigurationError, List[Region]] =
    ZIO.serviceWithZIO[RegistryPort](_.getActiveRegions)

  def getRegionStorage(region: Region): ZIO[RegistryPort, StorageError, Option[StorageLocation]] =
    ZIO.serviceWithZIO[RegistryPort](_.getRegionStorage(region))

  def registerTableLocationsBatch(
      registrations: List[(TableId, Region, String)]
  ): ZIO[RegistryPort, StorageError, BatchRegistrationResult] =
    ZIO.serviceWithZIO[RegistryPort](_.registerTableLocationsBatch(registrations))

  def getTableDataPathsBatch(
      requests: List[(TableId, Region)]
  ): ZIO[RegistryPort, StorageError, Map[(TableId, Region), Option[String]]] =
    ZIO.serviceWithZIO[RegistryPort](_.getTableDataPathsBatch(requests))

  def registerTableInRegionsBatch(
      tableId: TableId,
      regionPaths: List[(Region, String)]
  ): ZIO[RegistryPort, StorageError, BatchRegistrationResult] =
    ZIO.serviceWithZIO[RegistryPort](_.registerTableInRegionsBatch(tableId, regionPaths))
