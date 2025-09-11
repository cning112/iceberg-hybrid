package com.streamfirst.iceberg.hybrid.adapters

import com.streamfirst.iceberg.hybrid.domain.*
import com.streamfirst.iceberg.hybrid.domain.DomainError.*
import com.streamfirst.iceberg.hybrid.ports.SyncPort
import zio.*
import zio.logging.*
import java.time.Instant
import scala.collection.concurrent.TrieMap

/**
 * In-memory implementation of SyncPort for testing and development.
 * NOT suitable for production use - data is lost on restart.
 */
final case class InMemorySyncAdapter() extends SyncPort:
  private val events = TrieMap[EventId, SyncEvent]()

  override def publishSyncEvent(event: SyncEvent): IO[SyncError, Unit] =
    ZIO.succeed {
      events.put(event.eventId, event)
      ()
    }.tap(_ => ZIO.logDebug(s"Published sync event ${event.eventId}"))

  override def getSyncEvents(predicate: SyncEvent => Boolean): IO[SyncError, List[SyncEvent]] =
    ZIO.succeed {
      events.values.filter(predicate).toList.sortBy(_.createdAt)
    }

  override def updateEventStatus(eventId: EventId, status: SyncEvent.Status): IO[SyncError, Unit] =
    ZIO.attempt {
      events.get(eventId) match
        case Some(event) =>
          val updatedEvent = event.withStatus(status, Instant.now())
          events.put(eventId, updatedEvent)
          ()
        case None =>
          throw new IllegalArgumentException(s"Event not found: $eventId")
    }.catchAll(error => 
      ZIO.fail(DomainError.SyncEventNotFound(eventId))
    ).tap(_ => ZIO.logDebug(s"Updated event ${eventId} status to $status"))

  override def createMetadataSyncEvent(
    metadata: TableMetadata,
    targetRegion: Region
  ): IO[SyncError, SyncEvent] =
    ZIO.succeed {
      val event = SyncEvent.create(
        SyncEvent.Type.MetadataSync,
        metadata.tableId,
        metadata.commitId,
        metadata.sourceRegion,
        targetRegion
      )
      events.put(event.eventId, event)
      event
    }.tap(event => ZIO.logDebug(s"Created metadata sync event ${event.eventId}"))

  override def createDataSyncEvent(
    metadata: TableMetadata,
    dataFiles: List[StoragePath],
    targetRegion: Region
  ): IO[SyncError, SyncEvent] =
    ZIO.succeed {
      val event = SyncEvent.create(
        SyncEvent.Type.DataSync,
        metadata.tableId,
        metadata.commitId,
        metadata.sourceRegion,
        targetRegion
      )
      events.put(event.eventId, event)
      event
    }.tap(event => ZIO.logDebug(s"Created data sync event ${event.eventId}"))

  override def retryFailedEvent(eventId: EventId): IO[SyncError, Unit] =
    ZIO.attempt {
      events.get(eventId) match
        case Some(event) if event.status == SyncEvent.Status.Failed =>
          val retriedEvent = event.withStatus(SyncEvent.Status.Pending, Instant.now())
          events.put(eventId, retriedEvent)
          ()
        case Some(event) =>
          throw new IllegalArgumentException(s"Event $eventId is not in failed state: ${event.status}")
        case None =>
          throw new IllegalArgumentException(s"Event not found: $eventId")
    }.catchAll(error =>
      ZIO.fail(DomainError.SyncEventNotFound(eventId))
    ).tap(_ => ZIO.logDebug(s"Retried failed event $eventId"))

object InMemorySyncAdapter:
  val live: ZLayer[Any, Nothing, SyncPort] =
    ZLayer.succeed(InMemorySyncAdapter())