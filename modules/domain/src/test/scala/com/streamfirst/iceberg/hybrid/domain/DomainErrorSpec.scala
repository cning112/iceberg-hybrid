package com.streamfirst.iceberg.hybrid.domain

import zio.test.*

object DomainErrorSpec extends ZIOSpecDefault:
  def spec = suite("DomainError")(
    suite("SyncError")(
      test("SyncEventNotFound should format message correctly") {
        val eventId = EventId.generate()
        val error = DomainError.SyncEventNotFound(eventId)
        assertTrue(
          error.message == s"Sync event not found: $eventId",
          error.code.contains("SYNC_EVENT_NOT_FOUND")
        )
      },
      
      test("InvalidSyncTransition should format message correctly") {
        val error = DomainError.InvalidSyncTransition(
          SyncEvent.Status.Pending, 
          SyncEvent.Status.Completed
        )
        assertTrue(
          error.message == "Invalid sync status transition from Pending to Completed",
          error.code.contains("INVALID_SYNC_TRANSITION")
        )
      },
      
      test("RegionNotAvailable should format message correctly") {
        val region = Region("test-region", "Test Region")
        val error = DomainError.RegionNotAvailable(region)
        assertTrue(
          error.message == "Region not available: test-region",
          error.code.contains("REGION_NOT_AVAILABLE")
        )
      },
      
      test("DataReplicationFailed should format message correctly") {
        val tableId = TableId("analytics", "events")
        val sourceRegion = Region.UsEast1
        val targetRegion = Region.EuWest1
        val reason = "Network timeout"
        
        val error = DomainError.DataReplicationFailed(tableId, sourceRegion, targetRegion, reason)
        assertTrue(
          error.message == "Data replication failed for table analytics.events from us-east-1 to eu-west-1: Network timeout",
          error.code.contains("DATA_REPLICATION_FAILED")
        )
      }
    ),
    
    suite("StorageError")(
      test("StorageLocationNotFound should format message correctly") {
        val region = Region("ap-south-1", "Asia Pacific (Mumbai)")
        val error = DomainError.StorageLocationNotFound(region)
        assertTrue(
          error.message == "Storage location not found for region: ap-south-1",
          error.code.contains("STORAGE_LOCATION_NOT_FOUND")
        )
      },
      
      test("FileNotFound should format message correctly") {
        val path = StoragePath("s3://bucket/data/file.parquet")
        val error = DomainError.FileNotFound(path)
        assertTrue(
          error.message == "File not found: s3://bucket/data/file.parquet",
          error.code.contains("FILE_NOT_FOUND")
        )
      },
      
      test("FileCopyFailed should format message correctly") {
        val source = StoragePath("s3://source/file.parquet")
        val target = StoragePath("s3://target/file.parquet")
        val reason = "Access denied"
        
        val error = DomainError.FileCopyFailed(source, target, reason)
        assertTrue(
          error.message == "Failed to copy file from s3://source/file.parquet to s3://target/file.parquet: Access denied",
          error.code.contains("FILE_COPY_FAILED")
        )
      }
    ),
    
    suite("CatalogError")(
      test("TableNotFound should format message correctly") {
        val tableId = TableId("warehouse", "products")
        val error = DomainError.TableNotFound(tableId)
        assertTrue(
          error.message == "Table not found: warehouse.products",
          error.code.contains("TABLE_NOT_FOUND")
        )
      },
      
      test("CommitNotFound should format message correctly") {
        val tableId = TableId("analytics", "logs")
        val commitId = CommitId.generate()
        val error = DomainError.CommitNotFound(tableId, commitId)
        assertTrue(
          error.message == s"Commit $commitId not found for table analytics.logs",
          error.code.contains("COMMIT_NOT_FOUND")
        )
      },
      
      test("CatalogUnavailable should format message correctly") {
        val reason = "Service temporarily down"
        val error = DomainError.CatalogUnavailable(reason)
        assertTrue(
          error.message == "Catalog unavailable: Service temporarily down",
          error.code.contains("CATALOG_UNAVAILABLE")
        )
      }
    ),
    
    suite("Generic errors")(
      test("ValidationError should format message correctly") {
        val error = DomainError.ValidationError("email", "Invalid email format")
        assertTrue(
          error.message == "Validation failed for field 'email': Invalid email format",
          error.code.contains("VALIDATION_ERROR")
        )
      },
      
      test("ConfigurationError should format message correctly") {
        val error = DomainError.ConfigurationError("database.url", "Missing required property")
        assertTrue(
          error.message == "Configuration error for 'database.url': Missing required property",
          error.code.contains("CONFIGURATION_ERROR")
        )
      }
    )
  )