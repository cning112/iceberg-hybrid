package com.streamfirst.iceberg.hybrid.application

import com.streamfirst.iceberg.hybrid.domain.*
import com.streamfirst.iceberg.hybrid.domain.DomainError.*
import com.streamfirst.iceberg.hybrid.ports.{CatalogPort, RegistryPort, StoragePort}
import zio.*
import zio.stream.ZStream
import zio.test.*

object ReadRouterSpec extends ZIOSpecDefault:
  
  class TestCatalogPort extends CatalogPort:
    private val metadata = scala.collection.mutable.Map[TableId, TableMetadata]()
    
    def getMetadata(tableId: TableId, commitId: CommitId): IO[CatalogError, Option[TableMetadata]] =
      ZIO.succeed(metadata.get(tableId))
    
    def getLatestMetadata(tableId: TableId): IO[CatalogError, Option[TableMetadata]] =
      ZIO.succeed(metadata.get(tableId))
      
    def commitMetadata(metadata: TableMetadata): IO[CatalogError, CommitId] =
      ZIO.succeed(CommitId.generate())
    
    def listTables(namespace: String): IO[CatalogError, List[TableId]] =
      ZIO.succeed(metadata.keys.filter(_.namespace == namespace).toList)
    
    def tableExists(tableId: TableId): IO[CatalogError, Boolean] =
      ZIO.succeed(metadata.contains(tableId))
    
    def createTable(metadata: TableMetadata): IO[CatalogError, Unit] =
      ZIO.succeed(this.metadata.put(metadata.tableId, metadata)).unit
    
    def dropTable(tableId: TableId): IO[CatalogError, Unit] =
      ZIO.succeed(this.metadata.remove(tableId)).unit
    
    def getCommitHistory(tableId: TableId): IO[CatalogError, List[CommitId]] =
      ZIO.succeed(List(CommitId.generate()))
    
    def getMetadataBatch(requests: List[(TableId, CommitId)]): IO[CatalogError, Map[(TableId, CommitId), TableMetadata]] =
      ZIO.succeed(Map.empty)

  class TestRegistryPort extends RegistryPort:
    private val tableRegions = scala.collection.mutable.Map[TableId, List[Region]]()
    private val dataPaths = scala.collection.mutable.Map[(TableId, Region), String]()
    private val activeRegions = List(Region.UsEast1, Region.EuWest1)
    
    def registerTableLocation(tableId: TableId, region: Region, dataPath: String): IO[StorageError, Unit] =
      ZIO.succeed {
        tableRegions.updateWith(tableId) {
          case Some(regions) => Some(region :: regions)
          case None => Some(List(region))
        }
        dataPaths.put((tableId, region), dataPath)
      }
    
    def getTableRegions(tableId: TableId): IO[StorageError, List[Region]] =
      ZIO.succeed(tableRegions.getOrElse(tableId, List.empty))
    
    def getTableDataPath(tableId: TableId, region: Region): IO[StorageError, Option[String]] =
      ZIO.succeed(dataPaths.get((tableId, region)))
    
    def getActiveRegions: IO[DomainError.ConfigurationError, List[Region]] =
      ZIO.succeed(activeRegions)
    
    def getRegionStorage(region: Region): IO[StorageError, Option[StorageLocation]] =
      if activeRegions.contains(region) then
        ZIO.succeed(Some(StorageLocation.s3(region, s"bucket-${region.id}")))
      else
        ZIO.succeed(None)
    
    def registerRegion(region: Region, storageLocation: StorageLocation): IO[DomainError.ConfigurationError, Unit] =
      ZIO.unit
    
    def updateRegionStatus(region: Region, status: com.streamfirst.iceberg.hybrid.ports.RegionStatus): IO[DomainError.ConfigurationError, Unit] =
      ZIO.unit
    
    def getRegionTables(region: Region): IO[StorageError, List[TableId]] =
      ZIO.succeed(tableRegions.filter(_._2.contains(region)).keys.toList)

  class TestStoragePort extends StoragePort:
    private val storageLocations = Map(
      Region.UsEast1 -> StorageLocation.s3(Region.UsEast1, "us-east-bucket"),
      Region.EuWest1 -> StorageLocation.s3(Region.EuWest1, "eu-west-bucket"),
      Region.ApNortheast1 -> StorageLocation.s3(Region.ApNortheast1, "ap-northeast-bucket")
    )
    
    def getStorageLocation(region: Region): IO[StorageError, StorageLocation] =
      storageLocations.get(region) match
        case Some(location) => ZIO.succeed(location)
        case None => ZIO.fail(DomainError.StorageLocationNotFound(region))
    
    // Stub implementations for required StoragePort methods
    def writeFile(location: StorageLocation, path: StoragePath, data: Array[Byte]): IO[StorageError, Unit] = ZIO.unit
    def writeFileStream(location: StorageLocation, path: StoragePath, data: ZStream[Any, Throwable, Byte]): IO[StorageError, Unit] = ZIO.unit
    def readFile(location: StorageLocation, path: StoragePath): IO[StorageError, Array[Byte]] = ZIO.succeed(Array.empty)
    def readFileStream(location: StorageLocation, path: StoragePath): ZStream[Any, StorageError, Byte] = ZStream.empty
    def copyFile(sourceLocation: StorageLocation, sourcePath: StoragePath, targetLocation: StorageLocation, targetPath: StoragePath): IO[StorageError, Unit] = ZIO.unit
    def deleteFile(location: StorageLocation, path: StoragePath): IO[StorageError, Unit] = ZIO.unit
    def fileExists(location: StorageLocation, path: StoragePath): IO[StorageError, Boolean] = ZIO.succeed(false)
    def listFiles(location: StorageLocation, directory: StoragePath, predicate: StoragePath => Boolean): IO[StorageError, List[StoragePath]] = ZIO.succeed(List.empty)
    def getFileMetadata(location: StorageLocation, path: StoragePath): IO[StorageError, com.streamfirst.iceberg.hybrid.ports.FileMetadata] = ZIO.fail(DomainError.FileNotFound(path))

  def spec = suite("ReadRouter")(
    test("should route read to preferred region when available") {
      val tableId = TableId("analytics", "events")
      val preferredRegion = Region.EuWest1
      val dataPath = "analytics/events/data"
      
      val program = for
        registry <- ZIO.service[RegistryPort]
        _ <- registry.registerTableLocation(tableId, Region.UsEast1, dataPath)
        _ <- registry.registerTableLocation(tableId, Region.EuWest1, dataPath)
        
        router <- ZIO.service[ReadRouter]
        location <- router.routeRead(tableId, Some(preferredRegion))
      yield location
      
      for
        result <- program.provide(
          ZLayer.succeed[CatalogPort](TestCatalogPort()),
          ZLayer.succeed[RegistryPort](TestRegistryPort()),
          ZLayer.succeed[StoragePort](TestStoragePort()),
          ReadRouter.live
        )
      yield assertTrue(
        result.region == preferredRegion,
        result.tableId == tableId,
        result.storageLocation.region == preferredRegion
      )
    },
    
    test("should fallback to optimal region when preferred region not available") {
      val tableId = TableId("warehouse", "products")
      val unavailableRegion = Region("unavailable-region", "Unavailable")
      val dataPath = "warehouse/products/data"
      
      val program = for
        registry <- ZIO.service[RegistryPort]
        _ <- registry.registerTableLocation(tableId, Region.UsEast1, dataPath)
        
        router <- ZIO.service[ReadRouter]
        location <- router.routeRead(tableId, Some(unavailableRegion))
      yield location
      
      for
        result <- program.provide(
          ZLayer.succeed[CatalogPort](TestCatalogPort()),
          ZLayer.succeed[RegistryPort](TestRegistryPort()),
          ZLayer.succeed[StoragePort](TestStoragePort()),
          ReadRouter.live
        )
      yield assertTrue(
        result.region == Region.UsEast1, // Should fallback to available region
        result.tableId == tableId
      )
    },
    
    test("should fail when table not found in any region") {
      val tableId = TableId("nonexistent", "table")
      
      val program = for
        router <- ZIO.service[ReadRouter]
        _ <- router.routeRead(tableId)
      yield ()
      
      for
        result <- program.flip.provide(
          ZLayer.succeed[CatalogPort](TestCatalogPort()),
          ZLayer.succeed[RegistryPort](TestRegistryPort()),
          ZLayer.succeed[StoragePort](TestStoragePort()),
          ReadRouter.live
        )
      yield assertTrue(result.isInstanceOf[DomainError.TableNotFound])
    },
    
    test("should choose optimal region based on scoring") {
      val tableId = TableId("logs", "application")
      val dataPath = "logs/application/data"
      
      val program = for
        registry <- ZIO.service[RegistryPort]
        _ <- registry.registerTableLocation(tableId, Region.UsEast1, dataPath)
        _ <- registry.registerTableLocation(tableId, Region.EuWest1, dataPath)
        
        router <- ZIO.service[ReadRouter]
        location <- router.routeRead(tableId) // No preference, should pick optimal
      yield location
      
      for
        result <- program.provide(
          ZLayer.succeed[CatalogPort](TestCatalogPort()),
          ZLayer.succeed[RegistryPort](TestRegistryPort()),
          ZLayer.succeed[StoragePort](TestStoragePort()),
          ReadRouter.live
        )
      yield assertTrue(
        List(Region.UsEast1, Region.EuWest1).contains(result.region), // Should pick one of the active regions
        result.tableId == tableId
      )
    },
    
    test("should get latest metadata for table") {
      val tableId = TableId("test", "metadata")
      val metadata = TableMetadata.create(tableId, Region.UsEast1, List(StoragePath("s3://test/path/data.parquet")), """{"type":"struct","fields":[]}""")
      
      val program = for
        catalog <- ZIO.service[CatalogPort]
        _ <- catalog.createTable(metadata)
        
        router <- ZIO.service[ReadRouter]
        retrieved <- router.getLatestMetadata(tableId)
      yield retrieved
      
      for
        result <- program.provide(
          ZLayer.succeed[CatalogPort](TestCatalogPort()),
          ZLayer.succeed[RegistryPort](TestRegistryPort()),
          ZLayer.succeed[StoragePort](TestStoragePort()),
          ReadRouter.live
        )
      yield assertTrue(result == metadata)
    },
    
    test("should get metadata for specific commit") {
      val tableId = TableId("test", "commit")
      val metadata = TableMetadata.create(tableId, Region.UsEast1, List(StoragePath("s3://test/path/data.parquet")), """{"type":"struct","fields":[]}""")
      val commitId = CommitId.generate()
      
      val program = for
        catalog <- ZIO.service[CatalogPort]
        _ <- catalog.createTable(metadata)
        
        router <- ZIO.service[ReadRouter]
        retrieved <- router.getMetadata(tableId, commitId)
      yield retrieved
      
      for
        result <- program.provide(
          ZLayer.succeed[CatalogPort](TestCatalogPort()),
          ZLayer.succeed[RegistryPort](TestRegistryPort()),
          ZLayer.succeed[StoragePort](TestStoragePort()),
          ReadRouter.live
        )
      yield assertTrue(result == metadata)
    }
  )