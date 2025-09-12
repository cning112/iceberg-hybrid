package com.streamfirst.iceberg.hybrid.domain

import zio.test.*
import java.time.Instant

object SyncEventSpec extends ZIOSpecDefault:
  def spec = suite("SyncEvent")(
    suite("create")(
      test("should create a sync event with pending status") {
        val tableId = TableId("analytics", "events")
        val commitId = CommitId.generate()
        val sourceRegion = Region.UsEast1
        val targetRegion = Region.EuWest1
        
        val syncEvent = SyncEvent.create(
          SyncEvent.Type.DataSync,
          tableId,
          commitId,
          sourceRegion,
          targetRegion
        )
        
        assertTrue(
          syncEvent.eventType == SyncEvent.Type.DataSync,
          syncEvent.tableId == tableId,
          syncEvent.commitId == commitId,
          syncEvent.sourceRegion == sourceRegion,
          syncEvent.targetRegion == targetRegion,
          syncEvent.status == SyncEvent.Status.Pending
        )
      }
    ),
    
    suite("withStatus")(
      test("should update status and timestamp") {
        val originalEvent = SyncEvent.create(
          SyncEvent.Type.MetadataSync,
          TableId("test", "table"),
          CommitId.generate(),
          Region.UsEast1,
          Region.ApNortheast1
        )
        
        val timestamp = Instant.now().plusSeconds(60)
        val updatedEvent = originalEvent.withStatus(SyncEvent.Status.InProgress, timestamp)
        
        assertTrue(
          updatedEvent.status == SyncEvent.Status.InProgress,
          updatedEvent.updatedAt == timestamp,
          updatedEvent.eventId == originalEvent.eventId,
          updatedEvent.createdAt == originalEvent.createdAt
        )
      },
      
      test("should update status with current timestamp when timestamp not provided") {
        val originalEvent = SyncEvent.create(
          SyncEvent.Type.CommitCompleted,
          TableId("warehouse", "sales"),
          CommitId.generate(),
          Region.EuWest1,
          Region.UsEast1
        )
        
        val updatedEvent = originalEvent.withStatus(SyncEvent.Status.Completed)
        
        assertTrue(
          updatedEvent.status == SyncEvent.Status.Completed,
          updatedEvent.updatedAt.isAfter(originalEvent.updatedAt),
          updatedEvent.eventId == originalEvent.eventId
        )
      }
    ),
    
    suite("Type enum")(
      test("should have all expected sync types") {
        val types = SyncEvent.Type.values
        assertTrue(
          types.contains(SyncEvent.Type.MetadataSync),
          types.contains(SyncEvent.Type.DataSync),
          types.contains(SyncEvent.Type.CommitCompleted)
        )
      }
    ),
    
    suite("Status enum")(
      test("should have all expected statuses") {
        val statuses = SyncEvent.Status.values
        assertTrue(
          statuses.contains(SyncEvent.Status.Pending),
          statuses.contains(SyncEvent.Status.InProgress),
          statuses.contains(SyncEvent.Status.Completed),
          statuses.contains(SyncEvent.Status.Failed)
        )
      }
    )
  )