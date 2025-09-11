package com.streamfirst.iceberg.hybrid.domain

import java.time.Instant

/** Represents a synchronization event for replicating data or metadata between regions. Events
  * track the progress of cross-region replication and provide audit trails for debugging
  * distributed synchronization issues.
  */
final case class SyncEvent(
  eventId: EventId,
  eventType: SyncEvent.Type,
  tableId: TableId,
  commitId: CommitId,
  sourceRegion: Region,
  targetRegion: Region,
  status: SyncEvent.Status,
  createdAt: Instant,
  updatedAt: Instant
):
  /** Creates a copy of this event with updated status and timestamp. Convenience method for status
    * transitions.
    */
  def withStatus(newStatus: SyncEvent.Status, timestamp: Instant): SyncEvent =
    copy(status = newStatus, updatedAt = timestamp)

  def withStatus(newStatus: SyncEvent.Status): SyncEvent =
    withStatus(newStatus, Instant.now())

object SyncEvent:
  /** Types of synchronization events in the geo-distributed system.
    */
  enum Type:
    /** Replicate table metadata to another region */
    case MetadataSync

    /** Replicate data files to another region */
    case DataSync

    /** Notify that a commit has been completed globally */
    case CommitCompleted

  /** Current status of the synchronization event.
    */
  enum Status:
    /** Event created but not yet processed */
    case Pending

    /** Event is currently being processed */
    case InProgress

    /** Event completed successfully */
    case Completed

    /** Event failed and requires intervention */
    case Failed

  def create(
    eventType: Type,
    tableId: TableId,
    commitId: CommitId,
    sourceRegion: Region,
    targetRegion: Region
  ): SyncEvent =
    val now = Instant.now()
    SyncEvent(
      eventId = EventId.generate(),
      eventType = eventType,
      tableId = tableId,
      commitId = commitId,
      sourceRegion = sourceRegion,
      targetRegion = targetRegion,
      status = Status.Pending,
      createdAt = now,
      updatedAt = now
    )
