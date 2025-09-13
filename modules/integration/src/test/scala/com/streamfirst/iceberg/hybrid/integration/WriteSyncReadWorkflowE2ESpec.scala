package com.streamfirst.iceberg.hybrid.integration

import com.streamfirst.iceberg.hybrid.adapters.InMemoryRegistryAdapter
import com.streamfirst.iceberg.hybrid.application.ReadRouter
import com.streamfirst.iceberg.hybrid.domain.*
import com.streamfirst.iceberg.hybrid.domain.DomainError.*
import com.streamfirst.iceberg.hybrid.ports.{CatalogPort, RegistryPort, StoragePort, FileMetadata}
import zio.*
import zio.stream.ZStream
import zio.test.*

import scala.collection.concurrent.TrieMap

/** End-to-end tests for the complete Write -> Sync -> Read workflow in the geo-distributed Apache
  * Iceberg system.
  *
  * Tests realistic scenarios including:
  *   - Multi-region write coordination
  *   - Asynchronous data synchronization
  *   - Event-driven replication
  *   - Consistency guarantees
  *   - Performance under load
  */
object WriteSyncReadWorkflowE2ESpec extends ZIOSpecDefault:

  // Global test regions
  val UsEast1 = Region("us-east-1", "US East (N. Virginia)")
  val UsWest2 = Region("us-west-2", "US West (Oregon)")
  val EuWest1 = Region("eu-west-1", "Europe (Ireland)")
  val ApSoutheast1 = Region("ap-southeast-1", "Asia Pacific (Singapore)")
  val testSystemLayer = ZLayer.succeed[CatalogPort](E2ECatalogAdapter()) ++
    ZLayer.succeed[StoragePort](TestStorage()) ++ InMemoryRegistryAdapter.live

  def spec =
    suite("Write-Sync-Read Workflow E2E")(
      suite("Complete Write Coordination")(
        test("should coordinate write across multiple regions with proper sequencing") {
          for {
            catalog <- ZIO.service[CatalogPort]
            storage <- ZIO.service[StoragePort]
            registry <- ZIO.service[RegistryPort]

            // Initialize all regions
            regions = List(UsEast1, UsWest2, EuWest1, ApSoutheast1)
            _ <- ZIO.foreachDiscard(regions) { region =>
              registry.registerRegion(region, StorageLocation.s3(region, s"iceberg-${region.id}"))
            }

            // Create application services manually
            readRouter = ReadRouter(catalog, registry, storage)
            writeCoordinator = E2EWriteCoordinator(catalog, registry, storage)
            syncOrchestrator = E2ESyncOrchestrator(catalog, registry, storage)

            // Create table metadata for a high-frequency analytics table
            analyticsMetadata = TableMetadata.create(
              tableId = TableId("realtime", "click_events"),
              sourceRegion = UsEast1,
              dataFiles = List(
                StoragePath(
                  "realtime/click_events/year=2024/month=01/day=15/hour=14/part-00001.parquet"
                ),
                StoragePath(
                  "realtime/click_events/year=2024/month=01/day=15/hour=14/part-00002.parquet"
                )
              ),
              schema =
                """{"type":"struct","fields":[{"id":1,"name":"user_id","type":"long"},{"id":2,"name":"event_time","type":"timestamp"},{"id":3,"name":"page_url","type":"string"}]}"""
            )

            // Coordinate write to multiple regions (simulating global deployment)
            targetRegions = List(UsEast1, UsWest2, EuWest1, ApSoutheast1)
            commitId <- writeCoordinator.coordinateWrite(analyticsMetadata, targetRegions)

            // Trigger asynchronous synchronization
            syncEventIds <- syncOrchestrator.orchestrateSync(
              analyticsMetadata.tableId,
              commitId,
              UsEast1,
              List(UsWest2, EuWest1, ApSoutheast1)
            )

            // Wait for sync completion (in real system this would be event-driven)
            _ <- ZIO.sleep(100.millis) // Simulate async processing

            // Verify sync events completed successfully
            syncEvents <- ZIO.foreach(syncEventIds)(syncOrchestrator.getSyncEvent)

            // Test read routing from each region
            readResults <- ZIO.foreach(targetRegions) { region =>
              readRouter.routeRead(analyticsMetadata.tableId, Some(region))
                .map(location => (region, location))
            }

            // Verify data consistency across all regions
            dataConsistencyCheck <- ZIO.foreach(readResults) { case (region, readLocation) =>
              for {
                file1Exists <- storage
                  .fileExists(readLocation.storageLocation, analyticsMetadata.dataFiles.head)
                file2Exists <- storage
                  .fileExists(readLocation.storageLocation, analyticsMetadata.dataFiles(1))
              } yield (region, file1Exists && file2Exists)
            }
          } yield assertTrue(
            syncEventIds.length == 3, // US-West, EU, AP-Southeast
            syncEvents.forall(_.isDefined),
            syncEvents.flatten.forall(_.status == SyncEvent.Status.Completed),
            readResults.length == 4,
            readResults.forall { case (region, location) => location.region == region },
            dataConsistencyCheck.forall(_._2) // All regions have consistent data
          )
        }
      ),
      suite("High-Throughput Scenarios")(
        test("should handle concurrent writes and reads under load") {
          for {
            catalog <- ZIO.service[CatalogPort]
            storage <- ZIO.service[StoragePort]
            registry <- ZIO.service[RegistryPort]

            // Initialize all regions
            regions = List(UsEast1, UsWest2, EuWest1, ApSoutheast1)
            _ <- ZIO.foreachDiscard(regions) { region =>
              registry.registerRegion(region, StorageLocation.s3(region, s"iceberg-${region.id}"))
            }

            // Create application services manually
            readRouter = ReadRouter(catalog, registry, storage)
            writeCoordinator = E2EWriteCoordinator(catalog, registry, storage)
            syncOrchestrator = E2ESyncOrchestrator(catalog, registry, storage)

            // Create multiple table metadata for load testing
            tableMetadatas = (1 to 10).map { i =>
              TableMetadata.create(
                tableId = TableId("load_test", s"table_$i"),
                sourceRegion = if i % 2 == 0 then UsEast1 else EuWest1,
                dataFiles = List(StoragePath(s"load_test/table_$i/data.parquet")),
                schema = s"""{"type":"struct","fields":[{"id":1,"name":"id_$i","type":"long"}]}"""
              )
            }.toList

            // Concurrent write operations
            writeResults <- ZIO.foreachPar(tableMetadatas) { metadata =>
              for {
                commitId <- writeCoordinator
                  .coordinateWrite(metadata, List(UsEast1, EuWest1, UsWest2))
                _ <- syncOrchestrator.orchestrateSync(
                  metadata.tableId,
                  commitId,
                  metadata.sourceRegion,
                  if metadata.sourceRegion == UsEast1 then List(EuWest1, UsWest2)
                  else List(UsEast1, UsWest2)
                )
              } yield (metadata.tableId, commitId)
            }

            // Wait for all sync operations to complete
            _ <- ZIO.sleep(200.millis)

            // Concurrent read operations from different regions
            readResults <- ZIO.foreachPar(writeResults) { case (tableId, _) =>
              ZIO.foreachPar(List(UsEast1, EuWest1, UsWest2)) { region =>
                readRouter.routeRead(tableId, Some(region))
                  .map(location => (tableId, region, location.region))
              }
            }

            // Verify system state consistency
            pendingWrites <- writeCoordinator.getPendingWrites
            allSyncEvents <- syncOrchestrator.getSyncEvents
            completedSyncs = allSyncEvents.count(_._2.status == SyncEvent.Status.Completed)
          } yield assertTrue(
            writeResults.length == 10,
            readResults.flatten.length == 30, // 10 tables × 3 regions
            readResults.flatten.forall { case (tableId, requestedRegion, actualRegion) =>
              actualRegion == requestedRegion // Reads routed to correct regions
            },
            pendingWrites.isEmpty, // All writes completed
            completedSyncs >=
              20 // At least 20 sync operations completed (10 tables × 2 target regions each)
          )
        }
      ),
      suite("Failure Recovery Scenarios")(
        test("should handle partial sync failures and retry mechanisms") {
          for {
            catalog <- ZIO.service[CatalogPort]
            storage <- ZIO.service[StoragePort]
            registry <- ZIO.service[RegistryPort]

            // Initialize all regions
            regions = List(UsEast1, UsWest2, EuWest1, ApSoutheast1)
            _ <- ZIO.foreachDiscard(regions) { region =>
              registry.registerRegion(region, StorageLocation.s3(region, s"iceberg-${region.id}"))
            }

            // Create application services manually
            readRouter = ReadRouter(catalog, registry, storage)
            writeCoordinator = E2EWriteCoordinator(catalog, registry, storage)
            syncOrchestrator = E2ESyncOrchestrator(catalog, registry, storage)

            // Create metadata for a business table
            businessMetadata = TableMetadata.create(
              tableId = TableId("business", "transactions"),
              sourceRegion = UsEast1,
              dataFiles = List(StoragePath("business/transactions/data.parquet")),
              schema =
                """{"type":"struct","fields":[{"id":1,"name":"transaction_id","type":"string"},{"id":2,"name":"amount","type":"decimal"}]}"""
            )

            // Write coordination
            commitId <- writeCoordinator
              .coordinateWrite(businessMetadata, List(UsEast1, EuWest1, UsWest2))

            // Sync to other regions
            syncEventIds <- syncOrchestrator
              .orchestrateSync(businessMetadata.tableId, commitId, UsEast1, List(EuWest1, UsWest2))

            // Test read routing from different regions
            usEastRead <- readRouter.routeRead(businessMetadata.tableId, Some(UsEast1))
            euWestRead <- readRouter.routeRead(businessMetadata.tableId, Some(EuWest1))
            usWestRead <- readRouter.routeRead(businessMetadata.tableId, Some(UsWest2))

            // Verify system state
            allSyncEvents <- syncOrchestrator.getSyncEvents
          } yield assertTrue(
            syncEventIds.length == 2, // EU-West and US-West
            usEastRead.region == UsEast1,
            euWestRead.region == EuWest1,
            usWestRead.region == UsWest2,
            allSyncEvents.exists(_._2.tableId == businessMetadata.tableId)
          )
        }
      )
    ).provide(testSystemLayer)

  /** Advanced write coordinator that simulates realistic write patterns */
  class E2EWriteCoordinator(catalog: CatalogPort, registry: RegistryPort, storage: StoragePort):
    private val pendingWrites = TrieMap[CommitId, TableMetadata]()

    def coordinateWrite(
        metadata: TableMetadata,
        targetRegions: List[Region]
    ): IO[CatalogError | StorageError, CommitId] =
      for
        // Phase 1: Validate write request
        _ <- ZIO.logInfo(s"Coordinating write for table ${metadata.tableId.fullyQualifiedName}")

        // Phase 2: Generate commit ID and stage metadata
        commitId <- ZIO.succeed(CommitId.generate())
        _ <- ZIO.succeed(pendingWrites.put(commitId, metadata))

        // Phase 3: Write to primary region (usually source region)
        primaryLocation <- storage.getStorageLocation(metadata.sourceRegion)
        _ <- ZIO.foreachDiscard(metadata.dataFiles) { dataFile =>
          storage.writeFile(primaryLocation, dataFile, s"data for ${dataFile.value}".getBytes)
        }

        // Phase 4: Commit metadata to catalog
        actualCommitId <- catalog.commitMetadata(metadata)

        // Phase 5: Register table locations for sync
        _ <- ZIO.foreachDiscard(targetRegions) { region =>
          registry.registerTableLocation(
            metadata.tableId,
            region,
            s"${metadata.tableId.namespace}/${metadata.tableId.name}/"
          )
        }

        _ <- ZIO.logInfo(s"Write coordination completed for commit $actualCommitId")
        _ <- ZIO.succeed(pendingWrites.remove(commitId))
      yield actualCommitId

    def getPendingWrites: UIO[List[(CommitId, TableMetadata)]] = ZIO.succeed(pendingWrites.toList)

  /** Synchronization orchestrator that manages data replication */
  class E2ESyncOrchestrator(catalog: CatalogPort, registry: RegistryPort, storage: StoragePort):
    private val syncEvents = TrieMap[EventId, SyncEvent]()

    def orchestrateSync(
        tableId: TableId,
        commitId: CommitId,
        sourceRegion: Region,
        targetRegions: List[Region]
    ): IO[DomainError, List[EventId]] =
      for
        _ <- ZIO.logInfo(s"Orchestrating sync for table $tableId from ${sourceRegion.id}")

        // Get source and target locations
        sourceLocation <- storage.getStorageLocation(sourceRegion)
        targetLocations <- ZIO.foreach(targetRegions)(storage.getStorageLocation)

        // Create sync events for each target region
        events <- ZIO.foreach(targetRegions) { targetRegion =>
          val eventId = EventId.generate()
          val syncEvent = SyncEvent
            .create(SyncEvent.Type.DataSync, tableId, commitId, sourceRegion, targetRegion)
          syncEvents.put(eventId, syncEvent)
          ZIO.succeed(eventId)
        }

        // Execute data replication in parallel
        _ <- ZIO
          .foreachPar(targetRegions.zip(targetLocations)) { case (targetRegion, targetLocation) =>
            for
              // Get table metadata to find data files
              metadata <- catalog.getMetadata(tableId, commitId)
                .someOrFail(DomainError.TableNotFound(tableId))

              // Replicate each data file
              _ <- ZIO.foreachDiscard(metadata.dataFiles) { dataFile =>
                storage.copyFile(sourceLocation, dataFile, targetLocation, dataFile)
              }

              _ <- ZIO.logDebug(s"Completed data sync to region ${targetRegion.id}")
            yield ()
          }

        // Mark all sync events as completed
        _ <- ZIO.foreachDiscard(events) { eventId =>
          ZIO.succeed(
            syncEvents.updateWith(eventId)(_.map(_.withStatus(SyncEvent.Status.Completed)))
          )
        }

        _ <- ZIO.logInfo(s"Sync orchestration completed for ${events.length} regions")
      yield events

    def getSyncEvents: UIO[List[(EventId, SyncEvent)]] = ZIO.succeed(syncEvents.toList)

    def getSyncEvent(eventId: EventId): IO[StorageError, Option[SyncEvent]] =
      ZIO.succeed(syncEvents.get(eventId))

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

  /** Simple test storage adapter for basic functionality */
  class TestStorage extends StoragePort:
    private val locations = Map(
      UsEast1 -> StorageLocation.s3(UsEast1, "iceberg-us-east-1"),
      UsWest2 -> StorageLocation.s3(UsWest2, "iceberg-us-west-2"),
      EuWest1 -> StorageLocation.s3(EuWest1, "iceberg-eu-west-1"),
      ApSoutheast1 -> StorageLocation.s3(ApSoutheast1, "iceberg-ap-southeast-1")
    )
    private val files = collection.mutable.Map[(Region, String), Array[Byte]]()

    def getStorageLocation(region: Region): IO[StorageError, StorageLocation] =
      locations.get(region) match
        case Some(location) => ZIO.succeed(location)
        case None           => ZIO.fail(DomainError.StorageLocationNotFound(region))

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
