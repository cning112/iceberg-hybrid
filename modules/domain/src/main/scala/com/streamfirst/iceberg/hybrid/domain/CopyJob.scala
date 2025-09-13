package com.streamfirst.iceberg.hybrid.domain

import java.time.Instant

/** Represents the status of an async copy job */
enum CopyJobStatus:
  case Pending
  case InProgress
  case Completed 
  case Failed(error: String)

/** Represents an async file copy operation */
case class CopyJob(
  jobId: JobId,
  sourceLocation: StorageLocation,
  sourcePath: StoragePath,
  targetLocation: StorageLocation,
  targetPath: StoragePath,
  status: CopyJobStatus,
  createdAt: Instant,
  updatedAt: Instant,
  bytesToCopy: Option[Long] = None,
  bytesCopied: Option[Long] = None
):
  def withStatus(newStatus: CopyJobStatus): CopyJob = 
    this.copy(status = newStatus, updatedAt = Instant.now())
    
  def withProgress(bytesCopied: Long): CopyJob =
    this.copy(bytesCopied = Some(bytesCopied), updatedAt = Instant.now())
    
  def progressPercentage: Option[Double] = 
    for 
      total <- bytesToCopy
      copied <- bytesCopied
      if total > 0
    yield (copied.toDouble / total.toDouble) * 100.0

object CopyJob:
  def create(
    sourceLocation: StorageLocation,
    sourcePath: StoragePath, 
    targetLocation: StorageLocation,
    targetPath: StoragePath
  ): CopyJob =
    val now = Instant.now()
    CopyJob(
      jobId = JobId.generate(),
      sourceLocation = sourceLocation,
      sourcePath = sourcePath,
      targetLocation = targetLocation, 
      targetPath = targetPath,
      status = CopyJobStatus.Pending,
      createdAt = now,
      updatedAt = now
    )