package com.streamfirst.iceberg.hybrid.ports

import com.streamfirst.iceberg.hybrid.domain.*
import com.streamfirst.iceberg.hybrid.domain.DomainError.CatalogError
import zio.*
import zio.test.*

object CatalogPortSpec extends ZIOSpecDefault:
  
  class TestCatalogPort extends CatalogPort:
    private val tables = scala.collection.mutable.Map[TableId, TableMetadata]()
    private val commits = scala.collection.mutable.Map[(TableId, CommitId), TableMetadata]()
    
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
      if tables.contains(metadata.tableId) then
        ZIO.fail(DomainError.TableNotFound(metadata.tableId)) // Use appropriate catalog error
      else
        ZIO.succeed(tables.put(metadata.tableId, metadata)).unit
    
    def dropTable(tableId: TableId): IO[CatalogError, Unit] =
      ZIO.succeed(tables.remove(tableId)).unit
    
    def getCommitHistory(tableId: TableId): IO[CatalogError, List[CommitId]] =
      val tableCommits = commits.keys.filter(_._1 == tableId).map(_._2).toList
      ZIO.succeed(tableCommits)
    
    def getMetadataBatch(requests: List[(TableId, CommitId)]): IO[CatalogError, Map[(TableId, CommitId), TableMetadata]] =
      val results = requests.flatMap { case key@(tableId, commitId) =>
        commits.get(key).map(key -> _)
      }.toMap
      ZIO.succeed(results)

  def spec = suite("CatalogPort")(
    test("should create and retrieve table metadata") {
      val tableId = TableId("analytics", "events")
      val metadata = TableMetadata.create(
        tableId = tableId,
        sourceRegion = Region.UsEast1,
        dataFiles = List(StoragePath("s3://bucket/analytics/events/data.parquet")),
        schema = """{"type":"struct","fields":[]}"""
      )
      
      val program = for
        _ <- CatalogPort.createTable(metadata)
        retrieved <- CatalogPort.getLatestMetadata(tableId)
        exists <- CatalogPort.tableExists(tableId)
      yield (retrieved, exists)
      
      for
        result <- program.provide(ZLayer.succeed[CatalogPort](TestCatalogPort()))
        (retrieved, exists) = result
      yield assertTrue(
        retrieved.contains(metadata),
        exists
      )
    },
    
    test("should commit new metadata and create new version") {
      val tableId = TableId("warehouse", "products")
      val initialMetadata = TableMetadata.create(
        tableId = tableId,
        sourceRegion = Region.EuWest1,
        dataFiles = List(StoragePath("s3://bucket/warehouse/products/data.parquet")),
        schema = """{"type":"struct","fields":[]}"""
      )
      val updatedMetadata = TableMetadata.create(
        tableId = tableId,
        sourceRegion = Region.EuWest1,
        dataFiles = List(StoragePath("s3://bucket/warehouse/products/data2.parquet")),
        schema = """{"type":"struct","fields":[]}"""
      )
      
      val program = for
        _ <- CatalogPort.createTable(initialMetadata)
        commitId <- CatalogPort.commitMetadata(updatedMetadata)
        retrieved <- CatalogPort.getMetadata(tableId, commitId)
      yield retrieved
      
      for
        result <- program.provide(ZLayer.succeed[CatalogPort](TestCatalogPort()))
      yield assertTrue(result.contains(updatedMetadata))
    },
    
    test("should list tables in namespace") {
      val analyticsTable = TableId("analytics", "events")
      val warehouseTable = TableId("warehouse", "products")
      val analyticsMetadata = TableMetadata.create(
        tableId = analyticsTable,
        sourceRegion = Region.UsEast1,
        dataFiles = List(StoragePath("s3://bucket/analytics/events/data.parquet")),
        schema = """{"type":"struct","fields":[]}"""
      )
      val warehouseMetadata = TableMetadata.create(
        tableId = warehouseTable,
        sourceRegion = Region.UsEast1,
        dataFiles = List(StoragePath("s3://bucket/warehouse/products/data.parquet")),
        schema = """{"type":"struct","fields":[]}"""
      )
      
      val program = for
        _ <- CatalogPort.createTable(analyticsMetadata)
        _ <- CatalogPort.createTable(warehouseMetadata)
        analyticsTables <- CatalogPort.listTables("analytics")
        warehouseTables <- CatalogPort.listTables("warehouse")
      yield (analyticsTables, warehouseTables)
      
      for
        result <- program.provide(ZLayer.succeed[CatalogPort](TestCatalogPort()))
        (analyticsTables, warehouseTables) = result
      yield assertTrue(
        analyticsTables.contains(analyticsTable),
        !analyticsTables.contains(warehouseTable),
        warehouseTables.contains(warehouseTable),
        !warehouseTables.contains(analyticsTable)
      )
    },
    
    test("should track commit history") {
      val tableId = TableId("logs", "application")
      val metadata1 = TableMetadata.create(
        tableId = tableId,
        sourceRegion = Region.ApNortheast1,
        dataFiles = List(StoragePath("s3://bucket/logs/application/data1.parquet")),
        schema = """{"type":"struct","fields":[]}"""
      )
      val metadata2 = TableMetadata.create(
        tableId = tableId,
        sourceRegion = Region.ApNortheast1,
        dataFiles = List(StoragePath("s3://bucket/logs/application/data2.parquet")),
        schema = """{"type":"struct","fields":[]}"""
      )
      
      val program = for
        _ <- CatalogPort.createTable(metadata1)
        commit2 <- CatalogPort.commitMetadata(metadata2)
        history <- CatalogPort.getCommitHistory(tableId)
      yield (history, commit2)
      
      for
        result <- program.provide(ZLayer.succeed[CatalogPort](TestCatalogPort()))
        (history, commit2) = result
      yield assertTrue(
        history.contains(commit2),
        history.nonEmpty
      )
    },
    
    test("should handle batch metadata requests") {
      val tableId1 = TableId("batch", "table1")
      val tableId2 = TableId("batch", "table2")
      val metadata1 = TableMetadata.create(tableId1, Region.UsEast1, List(StoragePath("s3://bucket/batch/table1/data.parquet")), """{"type":"struct","fields":[]}""")
      val metadata2 = TableMetadata.create(tableId2, Region.UsEast1, List(StoragePath("s3://bucket/batch/table2/data.parquet")), """{"type":"struct","fields":[]}""")
      
      val program = for
        _ <- CatalogPort.createTable(metadata1)
        _ <- CatalogPort.createTable(metadata2)
        commit1 <- CatalogPort.commitMetadata(metadata1)
        commit2 <- CatalogPort.commitMetadata(metadata2)
        batch <- CatalogPort.getMetadataBatch(List((tableId1, commit1), (tableId2, commit2)))
      yield batch
      
      for
        result <- program.provide(ZLayer.succeed[CatalogPort](TestCatalogPort()))
      yield assertTrue(
        result.nonEmpty,
        result.keySet.exists(_._1 == tableId1),
        result.keySet.exists(_._1 == tableId2)
      )
    }
    
  ).provideLayerShared(ZLayer.succeed[CatalogPort](TestCatalogPort()))