package com.streamfirst.iceberg.hybrid.application

import com.streamfirst.iceberg.hybrid.domain.*
import com.streamfirst.iceberg.hybrid.domain.DomainError.*
import com.streamfirst.iceberg.hybrid.ports.*
import zio.*
import zio.logging.*

/**
 * Coordinates write operations across regions. Handles the distributed commit protocol
 * to ensure consistency when writing to geo-distributed tables.
 */
final case class WriteCoordinator(
  commitGatePort: CommitGatePort,
  catalogPort: CatalogPort,
  syncPort: SyncPort,
  registryPort: RegistryPort
):
  /**
   * Coordinates a write operation across all regions. Implements the distributed commit protocol.
   */
  def coordinateWrite(
    tableId: TableId,
    metadata: TableMetadata,
    sourceRegion: Region
  ): IO[SyncError | CatalogError, CommitId] =
    for
      _ <- ZIO.logInfo(s"Coordinating write for table $tableId from region ${sourceRegion.id}")
      
      // Step 1: Request commit approval from global gate
      request = CommitRequest.create(tableId, sourceRegion, metadata)
      approval <- commitGatePort.requestCommitApproval(request)
      
      // Step 2: Commit to local catalog first  
      actualCommitId <- catalogPort.commitMetadata(metadata)
      
      // Step 3: Create sync events for all other regions
      targetRegions = approval.approvedRegions.filter(_ != sourceRegion)
      _ <- ZIO.foreachParDiscard(targetRegions) { targetRegion =>
        for
          metadataEvent <- syncPort.createMetadataSyncEvent(metadata, targetRegion)
          dataEvent <- syncPort.createDataSyncEvent(metadata, metadata.dataFiles, targetRegion)
          _ <- syncPort.publishSyncEvent(metadataEvent)
          _ <- syncPort.publishSyncEvent(dataEvent)
        yield ()
      }
      
      // Step 4: Notify commit gate of successful completion
      _ <- commitGatePort.notifyCommitCompleted(actualCommitId, sourceRegion)
      
      _ <- ZIO.logInfo(s"Successfully coordinated write for table $tableId, commit $actualCommitId")
    yield actualCommitId

  /**
   * Handles a failed write by notifying the commit gate and triggering cleanup.
   */
  def handleWriteFailure(
    commitId: CommitId,
    region: Region,
    error: String
  ): IO[SyncError, Unit] =
    for
      _ <- ZIO.logError(s"Write failed for commit $commitId in region ${region.id}: $error")
      _ <- commitGatePort.notifyCommitFailed(commitId, region, error)
      // TODO: Implement rollback logic if needed
    yield ()

  /**
   * Gets the current status of a write operation.
   */
  def getWriteStatus(commitId: CommitId): IO[SyncError, CommitStatus] =
    commitGatePort.getCommitStatus(commitId)

object WriteCoordinator:
  val live: ZLayer[CommitGatePort & CatalogPort & SyncPort & RegistryPort, Nothing, WriteCoordinator] =
    ZLayer {
      for
        commitGatePort <- ZIO.service[CommitGatePort]
        catalogPort <- ZIO.service[CatalogPort]
        syncPort <- ZIO.service[SyncPort]
        registryPort <- ZIO.service[RegistryPort]
      yield WriteCoordinator(commitGatePort, catalogPort, syncPort, registryPort)
    }