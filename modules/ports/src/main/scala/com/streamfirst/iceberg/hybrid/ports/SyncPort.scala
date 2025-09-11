package com.streamfirst.iceberg.hybrid.ports

import com.streamfirst.iceberg.hybrid.domain.*
import com.streamfirst.iceberg.hybrid.domain.DomainError.SyncError
import zio.*

/**
 * Port for managing inter-region synchronization events. Coordinates the replication of metadata
 * and data between geographic regions using ZIO effects for type-safe error handling and composability.
 */
trait SyncPort:
  /**
   * Publishes a synchronization event to trigger replication.
   */
  def publishSyncEvent(event: SyncEvent): IO[SyncError, Unit]

  /**
   * Gets sync events matching the specified criteria. Provides flexible querying for complex
   * synchronization scenarios.
   */
  def getSyncEvents(predicate: SyncEvent => Boolean): IO[SyncError, List[SyncEvent]]

  /**
   * Gets all pending sync events for a target region. Used by regional sync workers to process
   * their queue.
   */
  def getPendingEvents(targetRegion: Region): IO[SyncError, List[SyncEvent]] =
    getSyncEvents { event =>
      event.targetRegion == targetRegion && event.status == SyncEvent.Status.Pending
    }

  /**
   * Updates the status of a synchronization event.
   */
  def updateEventStatus(eventId: EventId, status: SyncEvent.Status): IO[SyncError, Unit]

  /**
   * Creates a metadata synchronization event. This triggers replication of table metadata to the
   * target region.
   */
  def createMetadataSyncEvent(
    metadata: TableMetadata, 
    targetRegion: Region
  ): IO[SyncError, SyncEvent]

  /**
   * Creates a data synchronization event. This triggers replication of data files to the target
   * region.
   */
  def createDataSyncEvent(
    metadata: TableMetadata,
    dataFiles: List[StoragePath],
    targetRegion: Region
  ): IO[SyncError, SyncEvent]

  /**
   * Gets the synchronization history for a table in a specific region.
   */
  def getEventHistory(tableId: TableId, region: Region): IO[SyncError, List[SyncEvent]] =
    getSyncEvents { event =>
      event.tableId == tableId && event.targetRegion == region
    }

  /**
   * Gets all failed synchronization events for a region. Used for monitoring and manual
   * intervention.
   */
  def getFailedEvents(region: Region): IO[SyncError, List[SyncEvent]] =
    getSyncEvents { event =>
      event.targetRegion == region && event.status == SyncEvent.Status.Failed
    }

  /**
   * Retries a failed synchronization event. Resets the event status to PENDING for reprocessing.
   */
  def retryFailedEvent(eventId: EventId): IO[SyncError, Unit]

object SyncPort:
  /**
   * ZIO service accessor for dependency injection
   */
  def publishSyncEvent(event: SyncEvent): ZIO[SyncPort, SyncError, Unit] =
    ZIO.serviceWithZIO[SyncPort](_.publishSyncEvent(event))

  def getSyncEvents(predicate: SyncEvent => Boolean): ZIO[SyncPort, SyncError, List[SyncEvent]] =
    ZIO.serviceWithZIO[SyncPort](_.getSyncEvents(predicate))

  def getPendingEvents(targetRegion: Region): ZIO[SyncPort, SyncError, List[SyncEvent]] =
    ZIO.serviceWithZIO[SyncPort](_.getPendingEvents(targetRegion))

  def updateEventStatus(eventId: EventId, status: SyncEvent.Status): ZIO[SyncPort, SyncError, Unit] =
    ZIO.serviceWithZIO[SyncPort](_.updateEventStatus(eventId, status))

  def createMetadataSyncEvent(
    metadata: TableMetadata, 
    targetRegion: Region
  ): ZIO[SyncPort, SyncError, SyncEvent] =
    ZIO.serviceWithZIO[SyncPort](_.createMetadataSyncEvent(metadata, targetRegion))

  def createDataSyncEvent(
    metadata: TableMetadata,
    dataFiles: List[StoragePath],
    targetRegion: Region
  ): ZIO[SyncPort, SyncError, SyncEvent] =
    ZIO.serviceWithZIO[SyncPort](_.createDataSyncEvent(metadata, dataFiles, targetRegion))

  def retryFailedEvent(eventId: EventId): ZIO[SyncPort, SyncError, Unit] =
    ZIO.serviceWithZIO[SyncPort](_.retryFailedEvent(eventId))