package com.streamfirst.iceberg.hybrid.application

import com.streamfirst.iceberg.hybrid.domain.DomainError.{CatalogError, SyncError}
import com.streamfirst.iceberg.hybrid.domain.{
  CommitId,
  CommitRequest,
  DomainError,
  Region,
  TableId,
  TableMetadata,
  WriteJob,
  WriteJobId,
  WriteJobStatus
}
import com.streamfirst.iceberg.hybrid.ports.{
  CatalogPort,
  CommitGatePort,
  CommitStatus,
  RegistryPort,
  SyncPort
}
import zio.{IO, Ref, ZIO, ZLayer}

/** Coordinates write operations across regions. Handles the distributed commit protocol to ensure
  * consistency when writing to geo-distributed tables.
  */
final case class WriteCoordinator(
    commitGatePort: CommitGatePort,
    catalogPort: CatalogPort,
    syncPort: SyncPort,
    registryPort: RegistryPort,
    writeJobsRef: Ref[Map[WriteJobId, WriteJob]]
):
  /** Coordinates a write operation across all regions with job tracking. Implements the distributed commit protocol. */
  def coordinateWrite(
      tableId: TableId,
      metadata: TableMetadata,
      sourceRegion: Region
  ): IO[SyncError | CatalogError, WriteJobId] =
    val writeJob = WriteJob.create(tableId, sourceRegion)
    val request = CommitRequest.create(tableId, sourceRegion, metadata)
    
    for
      // Create and track the write job
      _ <- writeJobsRef.update(_.updated(writeJob.writeJobId, writeJob))
      _ <- ZIO.logInfo(s"Created write job ${writeJob.writeJobId} for table $tableId from region ${sourceRegion.id}")

      // Step 1: Request commit approval from global gate
      _ <- updateWriteJobStatus(writeJob.writeJobId, WriteJobStatus.RequestingApproval)
      approval <- commitGatePort.requestCommitApproval(request)
      targetRegions = approval.approvedRegions.filter(_ != sourceRegion)
      _ <- updateWriteJobTargetRegions(writeJob.writeJobId, targetRegions)
      _ <- updateWriteJobStatus(writeJob.writeJobId, WriteJobStatus.Approved)

      // Step 2: Commit to local catalog first
      _ <- updateWriteJobStatus(writeJob.writeJobId, WriteJobStatus.CommittingLocal)
      actualCommitId <- catalogPort.commitMetadata(metadata)
      _ <- updateWriteJobCommitId(writeJob.writeJobId, actualCommitId)

      // Step 3: Create sync events for all other regions
      _ <- updateWriteJobStatus(writeJob.writeJobId, WriteJobStatus.SynchronizingRegions)
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
      _ <- updateWriteJobStatus(writeJob.writeJobId, WriteJobStatus.Completed)

      _ <- ZIO.logInfo(s"Successfully coordinated write job ${writeJob.writeJobId} for table $tableId, commit $actualCommitId")
    yield writeJob.writeJobId

  /** Handles a failed write by notifying the commit gate and triggering cleanup. */
  def handleWriteFailure(commitId: CommitId, region: Region, error: String): IO[SyncError, Unit] =
    for
      _ <- ZIO.logError(s"Write failed for commit $commitId in region ${region.id}: $error")
      _ <- commitGatePort.notifyCommitFailed(commitId, region, error)
    // TODO: Implement rollback logic if needed
    yield ()

  /** Gets the current status of a write operation. */
  def getWriteStatus(commitId: CommitId): IO[SyncError, CommitStatus] =
    commitGatePort.getCommitStatus(commitId)

  /** Gets the write job by its ID. */
  def getWriteJob(writeJobId: WriteJobId): IO[SyncError, Option[WriteJob]] =
    writeJobsRef.get.map(_.get(writeJobId))

  /** Lists all active write jobs. */
  def listActiveWriteJobs(): IO[SyncError, List[WriteJob]] =
    writeJobsRef.get.map(_.values.filterNot(job => job.status match {
      case WriteJobStatus.Completed | WriteJobStatus.Failed(_) => true
      case _ => false
    }).toList)

  // Helper methods for updating write job state
  private def updateWriteJobStatus(writeJobId: WriteJobId, status: WriteJobStatus): IO[SyncError, Unit] =
    writeJobsRef.update { jobs =>
      jobs.get(writeJobId) match {
        case Some(job) => jobs.updated(writeJobId, job.withStatus(status))
        case None => jobs
      }
    }

  private def updateWriteJobCommitId(writeJobId: WriteJobId, commitId: CommitId): IO[SyncError, Unit] =
    writeJobsRef.update { jobs =>
      jobs.get(writeJobId) match {
        case Some(job) => jobs.updated(writeJobId, job.withCommitId(commitId))
        case None => jobs
      }
    }

  private def updateWriteJobTargetRegions(writeJobId: WriteJobId, regions: List[Region]): IO[SyncError, Unit] =
    writeJobsRef.update { jobs =>
      jobs.get(writeJobId) match {
        case Some(job) => jobs.updated(writeJobId, job.withTargetRegions(regions))
        case None => jobs
      }
    }

object WriteCoordinator:
  val live
      : ZLayer[CommitGatePort & CatalogPort & SyncPort & RegistryPort, Nothing, WriteCoordinator] =
    ZLayer {
      for
        commitGatePort <- ZIO.service[CommitGatePort]
        catalogPort <- ZIO.service[CatalogPort]
        syncPort <- ZIO.service[SyncPort]
        registryPort <- ZIO.service[RegistryPort]
        writeJobsRef <- Ref.make(Map.empty[WriteJobId, WriteJob])
      yield WriteCoordinator(commitGatePort, catalogPort, syncPort, registryPort, writeJobsRef)
    }
