package com.streamfirst.iceberg.hybrid.adapters

import com.streamfirst.iceberg.hybrid.domain.*
import com.streamfirst.iceberg.hybrid.ports.{RegionStatus, RegistryPort}
import zio.*
import zio.test.*

object InMemoryRegistryAdapterSpec extends ZIOSpecDefault:
  def spec = suite("InMemoryRegistryAdapter")(
    suite("table operations")(
      test("should register and retrieve table locations") {
        val tableId = TableId("analytics", "events")
        val region = Region.UsEast1
        val dataPath = "tables/analytics/events"
        
        val program = for
          registry <- ZIO.service[RegistryPort]
          _ <- registry.registerTableLocation(tableId, region, dataPath)
          retrievedPath <- registry.getTableDataPath(tableId, region)
          regions <- registry.getTableRegions(tableId)
        yield (retrievedPath, regions)
        
        for
          result <- program.provide(InMemoryRegistryAdapter.live)
          (retrievedPath, regions) = result
        yield assertTrue(
          retrievedPath.contains(dataPath),
          regions.contains(region)
        )
      },
      
      test("should handle multiple regions for same table") {
        val tableId = TableId("warehouse", "products")
        val region1 = Region.UsEast1
        val region2 = Region.EuWest1
        val dataPath = "tables/warehouse/products"
        
        val program = for
          registry <- ZIO.service[RegistryPort]
          _ <- registry.registerTableLocation(tableId, region1, dataPath)
          _ <- registry.registerTableLocation(tableId, region2, dataPath)
          regions <- registry.getTableRegions(tableId)
        yield regions
        
        for
          result <- program.provide(InMemoryRegistryAdapter.live)
        yield assertTrue(
          result.contains(region1),
          result.contains(region2),
          result.length == 2
        )
      },
      
      test("should return empty list for unknown table") {
        val unknownTable = TableId("unknown", "table")
        
        val program = for
          registry <- ZIO.service[RegistryPort]
          regions <- registry.getTableRegions(unknownTable)
        yield regions
        
        for
          result <- program.provide(InMemoryRegistryAdapter.live)
        yield assertTrue(result.isEmpty)
      },
      
      test("should return None for unknown table in region") {
        val unknownTable = TableId("unknown", "table")
        val region = Region.UsEast1
        
        val program = for
          registry <- ZIO.service[RegistryPort]
          path <- registry.getTableDataPath(unknownTable, region)
        yield path
        
        for
          result <- program.provide(InMemoryRegistryAdapter.live)
        yield assertTrue(result.isEmpty)
      }
    ),
    
    suite("region operations")(
      test("should register region with storage location") {
        val region = Region("test-region", "Test Region")
        val storageLocation = StorageLocation.s3(region, "test-bucket", Some("test-storage"))
        
        val program = for
          registry <- ZIO.service[RegistryPort]
          _ <- registry.registerRegion(region, storageLocation)
          retrievedStorage <- registry.getRegionStorage(region)
          activeRegions <- registry.getActiveRegions
        yield (retrievedStorage, activeRegions)
        
        for
          result <- program.provide(InMemoryRegistryAdapter.live)
          (retrievedStorage, activeRegions) = result
        yield assertTrue(
          retrievedStorage.contains(storageLocation),
          activeRegions.contains(region)
        )
      },
      
      test("should update region status") {
        val region = Region("status-test", "Status Test Region")
        val storageLocation = StorageLocation.s3(region, "test-bucket", Some("test-storage"))
        
        val program = for
          registry <- ZIO.service[RegistryPort]
          _ <- registry.registerRegion(region, storageLocation)
          activeRegions1 <- registry.getActiveRegions
          _ <- registry.updateRegionStatus(region, RegionStatus.Inactive)
          activeRegions2 <- registry.getActiveRegions
        yield (activeRegions1, activeRegions2)
        
        for
          result <- program.provide(InMemoryRegistryAdapter.live)
          (activeRegions1, activeRegions2) = result
        yield assertTrue(
          activeRegions1.contains(region),
          !activeRegions2.contains(region)
        )
      },
      
      test("should get tables in region") {
        val region = Region.ApNortheast1
        val table1 = TableId("analytics", "events")
        val table2 = TableId("warehouse", "products")
        val dataPath = "test/path"
        
        val program = for
          registry <- ZIO.service[RegistryPort]
          _ <- registry.registerTableLocation(table1, region, dataPath)
          _ <- registry.registerTableLocation(table2, region, dataPath)
          _ <- registry.registerTableLocation(table1, Region.EuWest1, dataPath) // Different region
          tables <- registry.getRegionTables(region)
        yield tables
        
        for
          result <- program.provide(InMemoryRegistryAdapter.live)
        yield assertTrue(
          result.contains(table1),
          result.contains(table2),
          result.length == 2
        )
      }
    ),
    
    suite("sample data layer")(
      test("should have predefined sample data") {
        val program = for
          registry <- ZIO.service[RegistryPort]
          activeRegions <- registry.getActiveRegions
          usStorage <- registry.getRegionStorage(Region.UsEast1)
          euStorage <- registry.getRegionStorage(Region.EuWest1)
          sampleTable = TableId("analytics", "user_events")
          tablePath <- registry.getTableDataPath(sampleTable, Region.UsEast1)
        yield (activeRegions, usStorage, euStorage, tablePath)
        
        for
          result <- program.provide(InMemoryRegistryAdapter.withSampleData)
          (activeRegions, usStorage, euStorage, tablePath) = result
        yield assertTrue(
          activeRegions.contains(Region.UsEast1),
          activeRegions.contains(Region.EuWest1),
          usStorage.isDefined,
          euStorage.isDefined,
          tablePath.isDefined
        )
      }
    ),
    
    suite("concurrent operations")(
      test("should handle concurrent table registrations") {
        val tableId = TableId("concurrent", "test")
        val regions = List(Region.UsEast1, Region.EuWest1, Region.ApNortheast1)
        val dataPath = "concurrent/test/path"
        
        val program = for
          registry <- ZIO.service[RegistryPort]
          _ <- ZIO.foreachParDiscard(regions) { region =>
            registry.registerTableLocation(tableId, region, dataPath)
          }
          finalRegions <- registry.getTableRegions(tableId)
        yield finalRegions
        
        for
          result <- program.provide(InMemoryRegistryAdapter.live)
        yield assertTrue(
          result.length == regions.length,
          regions.forall(result.contains)
        )
      },
      
      test("should handle concurrent region registrations") {
        val regions = List(
          Region("region1", "Region 1"),
          Region("region2", "Region 2"),
          Region("region3", "Region 3")
        )
        
        val program = for
          registry <- ZIO.service[RegistryPort]
          _ <- ZIO.foreachParDiscard(regions) { region =>
            val storage = StorageLocation.s3(region, s"bucket-${region.id}", Some(s"storage-${region.id}"))
            registry.registerRegion(region, storage)
          }
          activeRegions <- registry.getActiveRegions
        yield activeRegions
        
        for
          result <- program.provide(InMemoryRegistryAdapter.live)
        yield assertTrue(
          result.length == regions.length,
          regions.forall(result.contains)
        )
      }
    )
  )