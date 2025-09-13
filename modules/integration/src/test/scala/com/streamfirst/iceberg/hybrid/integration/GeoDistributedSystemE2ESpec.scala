package com.streamfirst.iceberg.hybrid.integration

import com.streamfirst.iceberg.hybrid.adapters.InMemoryRegistryAdapter
import com.streamfirst.iceberg.hybrid.application.{ReadLocation, ReadRouter}
import com.streamfirst.iceberg.hybrid.domain.*
import com.streamfirst.iceberg.hybrid.domain.DomainError.*
import com.streamfirst.iceberg.hybrid.ports.{CatalogPort, RegistryPort, StoragePort, FileMetadata}
import zio.*
import zio.stream.ZStream
import zio.test.*

/** End-to-end integration tests for the geo-distributed Apache Iceberg system.
  *
  * Tests the complete workflow from write coordination through regional synchronization to
  * intelligent read routing across multiple geographic regions. Validates the system's behavior
  * under various scenarios including failures, network partitions, and recovery.
  */
object GeoDistributedSystemE2ESpec extends ZIOSpecDefault:

  // Test regions representing a global deployment
  val UsEast1 = Region("us-east-1", "US East (N. Virginia)")
  val EuWest1 = Region("eu-west-1", "Europe (Ireland)")
  val ApNortheast1 = Region("ap-northeast-1", "Asia Pacific (Tokyo)")
  val ApSouth1 = Region("ap-south-1", "Asia Pacific (Mumbai)")

  // Test tables for different scenarios
  val AnalyticsTable = TableId("analytics", "user_events")
  val WarehouseTable = TableId("warehouse", "product_catalog")
  val RealtimeTable = TableId("realtime", "click_stream")
  val testSystemLayer = ZLayer.succeed[CatalogPort](E2ECatalogAdapter()) ++
    ZLayer.succeed[StoragePort](TestStorageWithFailure()) ++ InMemoryRegistryAdapter.live

  def spec =
    suite("Geo-Distributed System E2E")(
      suite("Complete Write-to-Read Workflow")(
        test("should handle table creation and metadata replication across all regions") {
          for {
            catalog <- ZIO.service[CatalogPort]
            registry <- ZIO.service[RegistryPort]
            storage <- ZIO.service[StoragePort]

            // Setup multi-region configuration
            _ <- registry.registerRegion(UsEast1, StorageLocation.s3(UsEast1, "test-us-east-1"))
            _ <- registry.registerRegion(EuWest1, StorageLocation.s3(EuWest1, "test-eu-west-1"))
            _ <- registry
              .registerRegion(ApNortheast1, StorageLocation.s3(ApNortheast1, "test-ap-northeast-1"))
            _ <- registry.registerRegion(ApSouth1, StorageLocation.s3(ApSouth1, "test-ap-south-1"))

            // Create application services manually
            readRouter = ReadRouter(catalog, registry, storage)

            // Create table metadata
            metadata = TableMetadata.create(
              tableId = AnalyticsTable,
              sourceRegion = UsEast1,
              dataFiles = List(StoragePath("analytics/user_events/2024/01/data.parquet")),
              schema =
                """{"type":"struct","fields":[{"id":1,"name":"user_id","type":"string"},{"id":2,"name":"event_type","type":"string"}]}"""
            )

            // Step 1: Create table in primary region
            _ <- catalog.createTable(metadata)

            // Step 2: Simulate data file creation in primary region
            primaryLocation <- storage.getStorageLocation(UsEast1)
            sampleData = "sample parquet data".getBytes
            _ <- storage.writeFile(
              primaryLocation,
              StoragePath("analytics/user_events/2024/01/data.parquet"),
              sampleData
            )

            // Step 3: Register table in all regions (simulating sync)
            _ <- registry
              .registerTableLocation(AnalyticsTable, UsEast1, "analytics/user_events/2024/01/")
            _ <- registry
              .registerTableLocation(AnalyticsTable, EuWest1, "analytics/user_events/2024/01/")
            _ <- registry
              .registerTableLocation(AnalyticsTable, ApNortheast1, "analytics/user_events/2024/01/")

            // Step 4: Replicate data files to other regions (simulating background sync)
            euLocation <- storage.getStorageLocation(EuWest1)
            apLocation <- storage.getStorageLocation(ApNortheast1)
            _ <- storage.copyFile(
              primaryLocation,
              StoragePath("analytics/user_events/2024/01/data.parquet"),
              euLocation,
              StoragePath("analytics/user_events/2024/01/data.parquet")
            )
            _ <- storage.copyFile(
              primaryLocation,
              StoragePath("analytics/user_events/2024/01/data.parquet"),
              apLocation,
              StoragePath("analytics/user_events/2024/01/data.parquet")
            )

            // Step 5: Verify table exists in catalog
            tableExists <- catalog.tableExists(AnalyticsTable)
            latestMetadata <- catalog.getLatestMetadata(AnalyticsTable)

            // Step 6: Test read routing from different regions
            usReadLocation <- readRouter.routeRead(AnalyticsTable, Some(UsEast1))
            euReadLocation <- readRouter.routeRead(AnalyticsTable, Some(EuWest1))
            apReadLocation <- readRouter.routeRead(AnalyticsTable, Some(ApNortheast1))

            // Step 7: Verify data accessibility from all regions
            usData <- storage.readFile(
              usReadLocation.storageLocation,
              StoragePath("analytics/user_events/2024/01/data.parquet")
            )
            euData <- storage.readFile(
              euReadLocation.storageLocation,
              StoragePath("analytics/user_events/2024/01/data.parquet")
            )
            apData <- storage.readFile(
              apReadLocation.storageLocation,
              StoragePath("analytics/user_events/2024/01/data.parquet")
            )
          } yield assertTrue(
            tableExists,
            latestMetadata.isDefined,
            latestMetadata.get.tableId == AnalyticsTable,
            usReadLocation.region == UsEast1,
            euReadLocation.region == EuWest1,
            apReadLocation.region == ApNortheast1,
            usData sameElements euData,
            euData sameElements apData,
            new String(usData) == "sample parquet data"
          )
        }
      ),
      suite("Failure Scenarios and Recovery")(
        test("should handle region failures and intelligent read routing") {
          for {
            catalog <- ZIO.service[CatalogPort]
            registry <- ZIO.service[RegistryPort]
            storage <- ZIO.service[StoragePort]

            // Create application services manually
            readRouter = ReadRouter(catalog, registry, storage)

            // Setup multi-region configuration
            _ <- registry.registerRegion(UsEast1, StorageLocation.s3(UsEast1, "test-us-east-1"))
            _ <- registry.registerRegion(EuWest1, StorageLocation.s3(EuWest1, "test-eu-west-1"))
            _ <- registry
              .registerRegion(ApNortheast1, StorageLocation.s3(ApNortheast1, "test-ap-northeast-1"))
            _ <- registry.registerRegion(ApSouth1, StorageLocation.s3(ApSouth1, "test-ap-south-1"))

            // Setup table in multiple regions
            metadata = TableMetadata.create(
              WarehouseTable,
              EuWest1,
              List(StoragePath("warehouse/products/data.parquet")),
              """{"type":"struct","fields":[{"id":1,"name":"product_id","type":"string"}]}"""
            )
            _ <- catalog.createTable(metadata)
            _ <- registry.registerTableLocation(WarehouseTable, UsEast1, "warehouse/products/")
            _ <- registry.registerTableLocation(WarehouseTable, EuWest1, "warehouse/products/")
            _ <- registry.registerTableLocation(WarehouseTable, ApNortheast1, "warehouse/products/")

            // Test 1: Normal operation - should route to preferred region
            normalRead <- readRouter.routeRead(WarehouseTable, Some(EuWest1))

            // Test 2: Simulate EU region failure
            testStorage = storage.asInstanceOf[TestStorageWithFailure]
            _ <- testStorage.simulateRegionFailure(EuWest1)

            // Test 3: Should fallback to alternative region when preferred region fails
            failoverRead <- readRouter.routeRead(WarehouseTable, Some(EuWest1))

            // Test 4: Should still work with no preference when some regions are down
            intelligentRead <- readRouter.routeRead(WarehouseTable, None)

            // Test 5: Recovery - restore EU region
            _ <- testStorage.restoreRegion(EuWest1)
            recoveredRead <- readRouter.routeRead(WarehouseTable, Some(EuWest1))
          } yield assertTrue(
            normalRead.region == EuWest1,
            failoverRead.region != EuWest1, // Should fallback to US or AP
            List(UsEast1, ApNortheast1).contains(failoverRead.region),
            List(UsEast1, ApNortheast1).contains(intelligentRead.region),
            recoveredRead.region == EuWest1 // Should route back to preferred region after recovery
          )
        },
        test("should handle concurrent operations across multiple regions") {
          for {
            catalog <- ZIO.service[CatalogPort]
            registry <- ZIO.service[RegistryPort]

            // Create application services manually
            readRouter = ReadRouter(catalog, registry, null) // storage not needed for this test

            // Setup multi-region configuration
            _ <- registry.registerRegion(UsEast1, StorageLocation.s3(UsEast1, "test-us-east-1"))
            _ <- registry.registerRegion(EuWest1, StorageLocation.s3(EuWest1, "test-eu-west-1"))
            _ <- registry
              .registerRegion(ApNortheast1, StorageLocation.s3(ApNortheast1, "test-ap-northeast-1"))
            _ <- registry.registerRegion(ApSouth1, StorageLocation.s3(ApSouth1, "test-ap-south-1"))

            // Create multiple tables concurrently from different regions
            concurrentTables = List(
              (TableId("concurrent", "table1"), UsEast1),
              (TableId("concurrent", "table2"), EuWest1),
              (TableId("concurrent", "table3"), ApNortheast1),
              (TableId("concurrent", "table4"), ApSouth1)
            )

            // Concurrent table creation
            _ <- ZIO.foreachPar(concurrentTables) { case (tableId, sourceRegion) =>
              for
                metadata <- ZIO.succeed(TableMetadata.create(
                  tableId,
                  sourceRegion,
                  List(StoragePath(s"${tableId.namespace}/${tableId.name}/data.parquet")),
                  """{"type":"struct","fields":[]}"""
                ))
                _ <- catalog.createTable(metadata)
                _ <- registry.registerTableLocation(
                  tableId,
                  sourceRegion,
                  s"${tableId.namespace}/${tableId.name}/"
                )
              yield ()
            }

            // Concurrent read operations
            readResults <- ZIO.foreachPar(concurrentTables) { case (tableId, sourceRegion) =>
              readRouter.routeRead(tableId, Some(sourceRegion))
            }

            // Verify all operations completed successfully
            allTables <- catalog.listTables("concurrent")
          } yield assertTrue(
            readResults.length == 4,
            readResults.map(_.tableId).toSet == concurrentTables.map(_._1).toSet,
            allTables.length == 4,
            allTables.forall(_.namespace == "concurrent")
          )
        }
      )
    ).provide(testSystemLayer)

  /** Complete in-memory implementation of the catalog for E2E testing */
  class E2ECatalogAdapter extends CatalogPort:
    private val tables = scala.collection.concurrent.TrieMap[TableId, TableMetadata]()
    private val commits = scala.collection.concurrent.TrieMap[(TableId, CommitId), TableMetadata]()

    def getMetadata(tableId: TableId, commitId: CommitId): IO[CatalogError, Option[TableMetadata]] =
      ZIO.succeed(commits.get((tableId, commitId)))

    def getLatestMetadata(tableId: TableId): IO[CatalogError, Option[TableMetadata]] =
      ZIO.succeed(tables.get(tableId))

    def commitMetadata(metadata: TableMetadata): IO[CatalogError, CommitId] =
      for
        commitId <- ZIO.succeed(CommitId.generate())
        _ <- ZIO.succeed {
          tables.put(metadata.tableId, metadata)
          commits.put((metadata.tableId, commitId), metadata)
        }
      yield commitId

    def listTables(namespace: String): IO[CatalogError, List[TableId]] =
      ZIO.succeed(tables.keys.filter(_.namespace == namespace).toList)

    def tableExists(tableId: TableId): IO[CatalogError, Boolean] =
      ZIO.succeed(tables.contains(tableId))

    def createTable(metadata: TableMetadata): IO[CatalogError, Unit] =
      ZIO.succeed(tables.put(metadata.tableId, metadata)).unit

    def dropTable(tableId: TableId): IO[CatalogError, Unit] =
      ZIO.succeed(tables.remove(tableId)).unit

    def getCommitHistory(tableId: TableId): IO[CatalogError, List[CommitId]] =
      val tableCommits = commits.keys.filter(_._1 == tableId).map(_._2).toList
      ZIO.succeed(tableCommits)

    def getMetadataBatch(
        requests: List[(TableId, CommitId)]
    ): IO[CatalogError, Map[(TableId, CommitId), TableMetadata]] =
      val results = requests.flatMap { case key @ (tableId, commitId) =>
        commits.get(key).map(key -> _)
      }.toMap
      ZIO.succeed(results)

    // Missing async/streaming methods
    def listTablesPaginated(namespace: String, pagination: PaginationRequest): IO[CatalogError, PaginatedResult[TableId]] =
      val allTables = tables.keys.filter(_.namespace == namespace).toList
      val startIndex = pagination.continuationToken.map(_.toInt).getOrElse(0)
      val endIndex = math.min(startIndex + pagination.pageSize, allTables.size)
      val items = allTables.slice(startIndex, endIndex)
      val hasMore = endIndex < allTables.size
      val nextToken = if (hasMore) Some(endIndex.toString) else None
      ZIO.succeed(PaginatedResult(items, nextToken, hasMore))

    def listTablesStream(namespace: String): ZStream[Any, CatalogError, TableId] =
      ZStream.fromIterable(tables.keys.filter(_.namespace == namespace))

    def getCommitHistoryPaginated(tableId: TableId, pagination: PaginationRequest): IO[CatalogError, PaginatedResult[CommitId]] =
      val allCommits = commits.keys.filter(_._1 == tableId).map(_._2).toList
      val startIndex = pagination.continuationToken.map(_.toInt).getOrElse(0)
      val endIndex = math.min(startIndex + pagination.pageSize, allCommits.size)
      val items = allCommits.slice(startIndex, endIndex)
      val hasMore = endIndex < allCommits.size
      val nextToken = if (hasMore) Some(endIndex.toString) else None
      ZIO.succeed(PaginatedResult(items, nextToken, hasMore))

    def getCommitHistoryStream(tableId: TableId): ZStream[Any, CatalogError, CommitId] =
      ZStream.fromIterable(commits.keys.filter(_._1 == tableId).map(_._2))

  /** Creates test layer with storage that supports failure simulation */
  class TestStorageWithFailure extends StoragePort:
    private val failedRegions = collection.mutable.Set[Region]()
    private val locations = Map(
      UsEast1 -> StorageLocation.s3(UsEast1, "test-us-east-1"),
      EuWest1 -> StorageLocation.s3(EuWest1, "test-eu-west-1"),
      ApNortheast1 -> StorageLocation.s3(ApNortheast1, "test-ap-northeast-1"),
      ApSouth1 -> StorageLocation.s3(ApSouth1, "test-ap-south-1")
    )
    private val files = collection.mutable.Map[(Region, String), Array[Byte]]()

    def getStorageLocation(region: Region): IO[StorageError, StorageLocation] =
      if failedRegions.contains(region) then ZIO.fail(DomainError.StorageLocationNotFound(region))
      else
        locations.get(region) match
          case Some(location) => ZIO.succeed(location)
          case None           => ZIO.fail(DomainError.StorageLocationNotFound(region))

    def simulateRegionFailure(region: Region): UIO[Unit] =
      ZIO.succeed(failedRegions.add(region)).unit

    def restoreRegion(region: Region): UIO[Unit] = ZIO.succeed(failedRegions.remove(region)).unit

    def fileExists(location: StorageLocation, path: StoragePath): IO[StorageError, Boolean] =
      ZIO.succeed(files.contains((location.region, path.value)))

    def copyFile(
        sourceLocation: StorageLocation,
        sourcePath: StoragePath,
        targetLocation: StorageLocation,
        targetPath: StoragePath
    ): IO[StorageError, Unit] =
      for
        sourceData <- readFile(sourceLocation, sourcePath)
        _ <- writeFile(targetLocation, targetPath, sourceData)
      yield ()

    def writeFile(
        location: StorageLocation,
        path: StoragePath,
        data: Array[Byte]
    ): IO[StorageError, Unit] = ZIO.succeed(files.put((location.region, path.value), data)).unit

    def readFile(location: StorageLocation, path: StoragePath): IO[StorageError, Array[Byte]] =
      files.get((location.region, path.value)) match
        case Some(data) => ZIO.succeed(data)
        case None       => ZIO.fail(DomainError.FileNotFound(path))

    // Stub implementations for other required methods
    def writeFileStream(
        location: StorageLocation,
        path: StoragePath,
        data: ZStream[Any, Throwable, Byte]
    ): IO[StorageError, Unit] = ZIO.unit
    def readFileStream(
        location: StorageLocation,
        path: StoragePath
    ): ZStream[Any, StorageError, Byte] = ZStream.empty
    def deleteFile(location: StorageLocation, path: StoragePath): IO[StorageError, Unit] = ZIO.unit
    def listFiles(
        location: StorageLocation,
        directory: StoragePath,
        predicate: StoragePath => Boolean
    ): IO[StorageError, List[StoragePath]] = ZIO.succeed(List.empty)
    def getFileMetadata(
        location: StorageLocation,
        path: StoragePath
    ): IO[StorageError, FileMetadata] =
      ZIO.fail(DomainError.FileNotFound(path))

    // Missing async/streaming methods
    def copyFileAsync(
        sourceLocation: StorageLocation,
        sourcePath: StoragePath,
        targetLocation: StorageLocation,
        targetPath: StoragePath
    ): IO[StorageError, JobId] =
      ZIO.succeed(JobId.generate())

    def getCopyJobStatus(jobId: JobId): IO[StorageError, Option[CopyJob]] =
      ZIO.succeed(None)

    def cancelCopyJob(jobId: JobId): IO[StorageError, Boolean] =
      ZIO.succeed(false)

    def listFilesStream(
        location: StorageLocation,
        directory: StoragePath,
        predicate: StoragePath => Boolean
    ): ZStream[Any, StorageError, StoragePath] =
      ZStream.empty
