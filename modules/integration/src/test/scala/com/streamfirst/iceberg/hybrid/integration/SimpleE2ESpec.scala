package com.streamfirst.iceberg.hybrid.integration

import com.streamfirst.iceberg.hybrid.adapters.InMemoryRegistryAdapter
import com.streamfirst.iceberg.hybrid.application.ReadRouter
import com.streamfirst.iceberg.hybrid.domain.*
import com.streamfirst.iceberg.hybrid.domain.DomainError.*
import com.streamfirst.iceberg.hybrid.ports.{CatalogPort, RegistryPort, StoragePort, FileMetadata}
import zio.*
import zio.stream.ZStream
import zio.test.*
import zio.test.Assertion.*

/** Simplified end-to-end integration tests for the geo-distributed Apache Iceberg system.
  *
  * Focuses on core scenarios:
  *   1. Multi-region table management
  *   2. Intelligent read routing
  *   3. Failure handling and recovery
  *   4. Data consistency across regions
  */
object SimpleE2ESpec extends ZIOSpecDefault:

  // Test regions
  val UsEast1 = Region("us-east-1", "US East")
  val EuWest1 = Region("eu-west-1", "EU West")
  val ApNortheast1 = Region("ap-northeast-1", "AP Northeast")

  /** Creates test layers with all required services */
  val testSystemLayer = ZLayer.succeed[CatalogPort](TestCatalog()) ++
    ZLayer.succeed[StoragePort](TestStorage()) ++ InMemoryRegistryAdapter.live

  def spec =
    suite("Simple E2E Integration Tests")(
      test("should create table and route reads across multiple regions") {
        for
          catalog <- ZIO.service[CatalogPort]
          storage <- ZIO.service[StoragePort]
          registry <- ZIO.service[RegistryPort]
          readRouter = ReadRouter(catalog, registry, storage)

          // Setup multi-region configuration
          _ <- registry.registerRegion(UsEast1, StorageLocation.s3(UsEast1, "test-us-east-1"))
          _ <- registry.registerRegion(EuWest1, StorageLocation.s3(EuWest1, "test-eu-west-1"))
          _ <- registry
            .registerRegion(ApNortheast1, StorageLocation.s3(ApNortheast1, "test-ap-northeast-1"))

          // Create table metadata
          tableId = TableId("analytics", "user_events")
          metadata = TableMetadata.create(
            tableId = tableId,
            sourceRegion = UsEast1,
            dataFiles = List(StoragePath("analytics/user_events/data.parquet")),
            schema = """{"type":"struct","fields":[{"name":"user_id","type":"string"}]}"""
          )

          // Create table and register across regions
          _ <- catalog.createTable(metadata)
          _ <- registry.registerTableLocation(tableId, UsEast1, "analytics/user_events/")
          _ <- registry.registerTableLocation(tableId, EuWest1, "analytics/user_events/")
          _ <- registry.registerTableLocation(tableId, ApNortheast1, "analytics/user_events/")

          // Test read routing to different regions
          usRead <- readRouter.routeRead(tableId, Some(UsEast1))
          euRead <- readRouter.routeRead(tableId, Some(EuWest1))
          apRead <- readRouter.routeRead(tableId, Some(ApNortheast1))

          // Verify table metadata is accessible
          tableExists <- catalog.tableExists(tableId)
          latestMetadata <- catalog.getLatestMetadata(tableId)
        yield assertTrue(
          tableExists,
          latestMetadata.isDefined,
          usRead.region == UsEast1,
          euRead.region == EuWest1,
          apRead.region == ApNortheast1,
          usRead.tableId == tableId,
          euRead.tableId == tableId,
          apRead.tableId == tableId
        )
      },
      test("should handle region failures and intelligent fallback") {
        for
          catalog <- ZIO.service[CatalogPort]
          storage <- ZIO.service[StoragePort]
          registry <- ZIO.service[RegistryPort]
          readRouter = ReadRouter(catalog, registry, storage)

          // Setup system
          _ <- registry.registerRegion(UsEast1, StorageLocation.s3(UsEast1, "test-us-east-1"))
          _ <- registry.registerRegion(EuWest1, StorageLocation.s3(EuWest1, "test-eu-west-1"))

          tableId = TableId("warehouse", "products")
          metadata = TableMetadata.create(
            tableId,
            UsEast1,
            List(StoragePath("warehouse/products/data.parquet")),
            "schema"
          )

          _ <- catalog.createTable(metadata)
          _ <- registry.registerTableLocation(tableId, UsEast1, "warehouse/products/")
          _ <- registry.registerTableLocation(tableId, EuWest1, "warehouse/products/")

          // Normal operation - should route to preferred region
          normalRead <- readRouter.routeRead(tableId, Some(EuWest1))

          // Simulate EU region failure (cast to access test methods)
          testStorage = storage.asInstanceOf[TestStorage]
          _ <- testStorage.simulateFailure(EuWest1)

          // Should fallback to available region
          fallbackRead <- readRouter.routeRead(tableId, Some(EuWest1))

          // Restore region
          _ <- testStorage.restoreRegion(EuWest1)
          recoveredRead <- readRouter.routeRead(tableId, Some(EuWest1))
        yield assertTrue(
          normalRead.region == EuWest1,
          fallbackRead.region == UsEast1, // Should fallback to US when EU fails
          recoveredRead.region == EuWest1 // Should route back to EU after recovery
        )
      },
      test("should handle concurrent table operations") {
        for
          catalog <- ZIO.service[CatalogPort]
          registry <- ZIO.service[RegistryPort]
          storage <- ZIO.service[StoragePort]
          readRouter = ReadRouter(catalog, registry, storage)

          // Setup regions
          _ <- registry.registerRegion(UsEast1, StorageLocation.s3(UsEast1, "test-us-east-1"))
          _ <- registry.registerRegion(EuWest1, StorageLocation.s3(EuWest1, "test-eu-west-1"))

          // Create multiple tables concurrently
          tableIds = List(
            TableId("concurrent", "table1"),
            TableId("concurrent", "table2"),
            TableId("concurrent", "table3")
          )

          // Concurrent table creation and registration
          _ <- ZIO.foreachParDiscard(tableIds) { tableId =>
            for
              metadata <- ZIO.succeed(TableMetadata.create(
                tableId,
                UsEast1,
                List(StoragePath(s"${tableId.namespace}/${tableId.name}/data.parquet")),
                "schema"
              ))
              _ <- catalog.createTable(metadata)
              _ <- registry
                .registerTableLocation(tableId, UsEast1, s"${tableId.namespace}/${tableId.name}/")
              _ <- registry
                .registerTableLocation(tableId, EuWest1, s"${tableId.namespace}/${tableId.name}/")
            yield ()
          }

          // Concurrent read operations
          readResults <- ZIO.foreachPar(tableIds) { tableId =>
            readRouter.routeRead(tableId, Some(UsEast1))
          }

          // Verify all operations completed successfully
          allTables <- catalog.listTables("concurrent")
        yield assertTrue(
          readResults.length == 3,
          readResults.forall(_.region == UsEast1),
          allTables.length == 3,
          allTables.toSet == tableIds.toSet
        )
      }
    ).provide(testSystemLayer)

  /** Simple catalog implementation for testing */
  class TestCatalog extends CatalogPort:
    private val tables = collection.mutable.Map[TableId, TableMetadata]()
    private val commits = collection.mutable.Map[(TableId, CommitId), TableMetadata]()

    def createTable(metadata: TableMetadata): IO[CatalogError, Unit] =
      ZIO.succeed(tables.put(metadata.tableId, metadata)).unit

    def getLatestMetadata(tableId: TableId): IO[CatalogError, Option[TableMetadata]] =
      ZIO.succeed(tables.get(tableId))

    def getMetadata(tableId: TableId, commitId: CommitId): IO[CatalogError, Option[TableMetadata]] =
      ZIO.succeed(commits.get((tableId, commitId)))

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

    def dropTable(tableId: TableId): IO[CatalogError, Unit] =
      ZIO.succeed(tables.remove(tableId)).unit

    def getCommitHistory(tableId: TableId): IO[CatalogError, List[CommitId]] =
      ZIO.succeed(commits.keys.filter(_._1 == tableId).map(_._2).toList)

    def getMetadataBatch(
        requests: List[(TableId, CommitId)]
    ): IO[CatalogError, Map[(TableId, CommitId), TableMetadata]] =
      val results = requests.flatMap { key => commits.get(key).map(key -> _) }.toMap
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

  /** Simple storage implementation with failure simulation */
  class TestStorage extends StoragePort:
    private val locations = Map(
      UsEast1 -> StorageLocation.s3(UsEast1, "test-us-east-1"),
      EuWest1 -> StorageLocation.s3(EuWest1, "test-eu-west-1"),
      ApNortheast1 -> StorageLocation.s3(ApNortheast1, "test-ap-northeast-1")
    )
    private val files = collection.mutable.Map[(Region, String), Array[Byte]]()
    private val failedRegions = collection.mutable.Set[Region]()

    def getStorageLocation(region: Region): IO[StorageError, StorageLocation] =
      if failedRegions.contains(region) then ZIO.fail(DomainError.StorageLocationNotFound(region))
      else
        locations.get(region) match
          case Some(location) => ZIO.succeed(location)
          case None           => ZIO.fail(DomainError.StorageLocationNotFound(region))

    def simulateFailure(region: Region): UIO[Unit] = ZIO.succeed(failedRegions.add(region)).unit

    def restoreRegion(region: Region): UIO[Unit] = ZIO.succeed(failedRegions.remove(region)).unit

    def writeFile(
        location: StorageLocation,
        path: StoragePath,
        data: Array[Byte]
    ): IO[StorageError, Unit] = ZIO.succeed(files.put((location.region, path.value), data)).unit

    def readFile(location: StorageLocation, path: StoragePath): IO[StorageError, Array[Byte]] =
      files.get((location.region, path.value)) match
        case Some(data) => ZIO.succeed(data)
        case None       => ZIO.fail(DomainError.FileNotFound(path))

    def fileExists(location: StorageLocation, path: StoragePath): IO[StorageError, Boolean] =
      ZIO.succeed(files.contains((location.region, path.value)))

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
    def copyFile(
        sourceLocation: StorageLocation,
        sourcePath: StoragePath,
        targetLocation: StorageLocation,
        targetPath: StoragePath
    ): IO[StorageError, Unit] = ZIO.unit
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
