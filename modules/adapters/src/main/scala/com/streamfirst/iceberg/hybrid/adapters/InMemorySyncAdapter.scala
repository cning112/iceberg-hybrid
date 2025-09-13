package com.streamfirst.iceberg.hybrid.adapters

import com.streamfirst.iceberg.hybrid.domain.DomainError.SyncError
import com.streamfirst.iceberg.hybrid.domain.{
  DomainError,
  EventId,
  PaginatedResult,
  PaginationRequest,
  Region,
  StoragePath,
  SyncEvent,
  TableMetadata
}
import com.streamfirst.iceberg.hybrid.ports.SyncPort
import zio.stream.ZStream
import zio.{IO, Ref, ZIO, ZLayer}

import java.time.Instant

/** In-memory implementation of SyncPort for testing and development. NOT suitable for production
  * use - data is lost on restart. Uses ZIO Ref for thread-safe atomic operations.
  */
final case class InMemorySyncAdapter(events: Ref[Map[EventId, SyncEvent]]) extends SyncPort:

  override def publishSyncEvent(event: SyncEvent): IO[SyncError, Unit] =
    events.update(_ + (event.eventId -> event)) *>
      ZIO.logDebug(s"Published sync event ${event.eventId}")

  override def getSyncEvents(predicate: SyncEvent => Boolean): IO[SyncError, List[SyncEvent]] =
    events.get.map(_.values.filter(predicate).toList.sortBy(_.createdAt))

  override def updateEventStatus(eventId: EventId, status: SyncEvent.Status): IO[SyncError, Unit] =
    for {
      now <- ZIO.succeed(Instant.now())
      result <- events.modify { m =>
        m.get(eventId) match
          case Some(ev) =>
            val updated = ev.withStatus(status, now)
            (Right(()): Either[SyncError, Unit], m.updated(eventId, updated))
          case None => (Left(DomainError.SyncEventNotFound(eventId)), m)
      }
      _ <- ZIO.fromEither(result)
      _ <- ZIO.logDebug(s"Updated event $eventId status to $status")
    } yield ()

  override def createMetadataSyncEvent(
      metadata: TableMetadata,
      targetRegion: Region
  ): IO[SyncError, SyncEvent] =
    for {
      event <- ZIO.succeed(SyncEvent.create(
        SyncEvent.Type.MetadataSync,
        metadata.tableId,
        metadata.commitId,
        metadata.sourceRegion,
        targetRegion
      ))
      _ <- events.update(_.updated(event.eventId, event))
      _ <- ZIO.logDebug(s"Created metadata sync event ${event.eventId}")
    } yield event

  override def createDataSyncEvent(
      metadata: TableMetadata,
      dataFiles: List[StoragePath],
      targetRegion: Region
  ): IO[SyncError, SyncEvent] =
    for {
      event <- ZIO.succeed(SyncEvent.create(
        SyncEvent.Type.DataSync,
        metadata.tableId,
        metadata.commitId,
        metadata.sourceRegion,
        targetRegion
      ))
      _ <- events.update(_.updated(event.eventId, event))
      _ <- ZIO.logDebug(s"Created data sync event ${event.eventId}")
    } yield event

  override def retryFailedEvent(eventId: EventId): IO[SyncError, Unit] =
    for {
      now <- ZIO.succeed(Instant.now())
      result <- events.modify { m =>
        m.get(eventId) match
          case Some(ev) if ev.status == SyncEvent.Status.Failed =>
            val retried = ev.withStatus(SyncEvent.Status.Pending, now)
            (Right(()): Either[SyncError, Unit], m.updated(eventId, retried))
          case Some(ev) =>
            // keep map unchanged
            (Left(DomainError.SyncEventNotFound(eventId)): Either[SyncError, Unit], m)
          case None => (Left(DomainError.SyncEventNotFound(eventId)): Either[SyncError, Unit], m)
      }
      _ <- ZIO.fromEither(result)
      _ <- ZIO.logDebug(s"Retried failed event $eventId")
    } yield ()

  override def getSyncEventsStream(predicate: SyncEvent => Boolean): ZStream[Any, SyncError, SyncEvent] =
    ZStream.fromIterableZIO(
      events.get.map(_.values.filter(predicate).toList.sortBy(_.createdAt))
    )

  override def getSyncEventsPaginated(
      predicate: SyncEvent => Boolean,
      pagination: PaginationRequest
  ): IO[SyncError, PaginatedResult[SyncEvent]] =
    events.get.map { eventsMap =>
      val filteredEvents = eventsMap.values.filter(predicate).toList.sortBy(_.createdAt)
      val startIndex = pagination.continuationToken.flatMap(_.toIntOption).getOrElse(0)
      val endIndex = Math.min(startIndex + pagination.pageSize, filteredEvents.size)
      val pageEvents = filteredEvents.slice(startIndex, endIndex)
      val nextToken = if endIndex < filteredEvents.size then Some(endIndex.toString) else None
      
      PaginatedResult(
        items = pageEvents,
        continuationToken = nextToken,
        hasMore = endIndex < filteredEvents.size
      )
    }

object InMemorySyncAdapter:
  val live: ZLayer[Any, Nothing, SyncPort] = ZLayer.fromZIO {
    for { ref <- Ref.make(Map.empty[EventId, SyncEvent]) } yield InMemorySyncAdapter(ref)
  }
