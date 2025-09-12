package com.streamfirst.iceberg.hybrid.ports

import com.streamfirst.iceberg.hybrid.domain.*
import com.streamfirst.iceberg.hybrid.domain.DomainError.SyncError
import zio.{IO, ZIO}

/** Port for the global commit gate that ensures write consistency across regions. Coordinates
  * distributed consensus before allowing commits to proceed.
  */
trait CommitGatePort:
  /** Requests approval for a commit from the global commit gate. This ensures no conflicting writes
    * are happening across regions.
    */
  def requestCommitApproval(request: CommitRequest): IO[SyncError, CommitApproval]

  /** Notifies the commit gate that a commit has been completed in a region. Used for cleanup and
    * releasing locks.
    */
  def notifyCommitCompleted(commitId: CommitId, region: Region): IO[SyncError, Unit]

  /** Notifies the commit gate that a commit has failed in a region. Triggers rollback procedures
    * across other regions if needed.
    */
  def notifyCommitFailed(commitId: CommitId, region: Region, error: String): IO[SyncError, Unit]

  /** Gets the current status of a commit across all regions. */
  def getCommitStatus(commitId: CommitId): IO[SyncError, CommitStatus]

  /** Lists all pending commit requests. */
  def getPendingCommits: IO[SyncError, List[CommitRequest]]

  /** Cancels a pending commit request. */
  def cancelCommitRequest(requestId: String): IO[SyncError, Unit]

/** Represents the global status of a commit across all regions. */
final case class CommitStatus(
    commitId: CommitId,
    tableId: TableId,
    status: CommitStatus.Status,
    completedRegions: List[Region],
    failedRegions: List[Region],
    pendingRegions: List[Region]
)

object CommitStatus:
  enum Status:
    case Pending // Waiting for approval
    case Approved // Approved, executing across regions
    case Completed // Successfully completed in all regions
    case Failed // Failed in one or more regions
    case Cancelled // Cancelled before completion

object CommitGatePort:
  /** ZIO service accessors for dependency injection */
  def requestCommitApproval(
      request: CommitRequest
  ): ZIO[CommitGatePort, SyncError, CommitApproval] =
    ZIO.serviceWithZIO[CommitGatePort](_.requestCommitApproval(request))

  def notifyCommitCompleted(
      commitId: CommitId,
      region: Region
  ): ZIO[CommitGatePort, SyncError, Unit] =
    ZIO.serviceWithZIO[CommitGatePort](_.notifyCommitCompleted(commitId, region))

  def notifyCommitFailed(
      commitId: CommitId,
      region: Region,
      error: String
  ): ZIO[CommitGatePort, SyncError, Unit] =
    ZIO.serviceWithZIO[CommitGatePort](_.notifyCommitFailed(commitId, region, error))

  def getCommitStatus(commitId: CommitId): ZIO[CommitGatePort, SyncError, CommitStatus] =
    ZIO.serviceWithZIO[CommitGatePort](_.getCommitStatus(commitId))
