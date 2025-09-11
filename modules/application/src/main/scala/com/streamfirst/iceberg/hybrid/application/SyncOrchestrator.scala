package com.streamfirst.iceberg.hybrid.application

import com.streamfirst.iceberg.hybrid.domain.*
import com.streamfirst.iceberg.hybrid.domain.DomainError.*
import com.streamfirst.iceberg.hybrid.ports.*

/** Orchestrates synchronization operations between regions. Processes sync events and coordinates
  * data/metadata replication using ZIO effects for composable, type-safe operations.
  */
final case class SyncOrchestrator(
  syncPort: SyncPort,
  storagePort: StoragePort,
  catalogPort: CatalogPort,
  registryPort: RegistryPort
):
  /** Processes all pending sync events for a specific region. This method is typically called by a
    * scheduled worker process.
    */
  def processPendingEvents(region: Region): IO[SyncError, Int] =
    for
      _ <- ZIO.logInfo(s"Processing pending sync events for region ${region.id}")
      pendingEvents <- syncPort.getPendingEvents(region)
      results <- ZIO.foreachPar(pendingEvents)(processSyncEvent)
      successCount = results.count(identity)
      _ <- ZIO.logInfo(
        s"Processed $successCount out of ${pendingEvents.size} pending events for region ${region.id}")
    yield successCount

  /** Processes a single synchronization event based on its type.
    */
  private def processSyncEvent(event: SyncEvent): IO[SyncError, Boolean] =
    for
      _ <- ZIO.logDebug(s"Processing sync event: ${event.eventId}")
      _ <- syncPort.updateEventStatus(event.eventId, SyncEvent.Status.InProgress)
      success <- event.eventType match
        case SyncEvent.Type.MetadataSync => processMetadataSync(event)
        case SyncEvent.Type.DataSync => processDataSync(event)
        case SyncEvent.Type.CommitCompleted => processCommitCompleted(event)
      finalStatus = if success then SyncEvent.Status.Completed else SyncEvent.Status.Failed
      _ <- syncPort.updateEventStatus(event.eventId, finalStatus)
      _ <-
        if success then ZIO.logDebug(s"Successfully processed sync event ${event.eventId}")
        else ZIO.logWarning(s"Failed to process sync event ${event.eventId}")
    yield success

  /** Processes metadata synchronization by fetching and storing metadata locally.
    */
  private def processMetadataSync(event: SyncEvent): IO[SyncError, Boolean] =
    for
      _ <- ZIO.logDebug(
        s"Processing metadata sync for table ${event.tableId} to region ${event.targetRegion.id}")
      metadataOpt <- catalogPort
        .getMetadata(event.tableId, event.commitId)
        .mapError(catalogErrorToSyncError)
      metadata <- ZIO
        .fromOption(metadataOpt)
        .orElseFail(DomainError.SyncEventNotFound(event.eventId))

      // Register table location in target region if not already present
      existingPath <- registryPort
        .getTableDataPath(event.tableId, event.targetRegion)
        .mapError(storageErrorToSyncError)
      _ <- existingPath match
        case Some(_) => ZIO.unit
        case None =>
          val dataPath = generateTableDataPath(event.tableId)
          registryPort
            .registerTableLocation(event.tableId, event.targetRegion, dataPath)
            .mapError(storageErrorToSyncError) *>
            ZIO.logDebug(
              s"Registered table location for ${event.tableId} in region ${event.targetRegion.id}")
    yield true

  /** Generates a standard data path for table data in a region.
    */
  private def generateTableDataPath(tableId: TableId): String =
    s"tables/${tableId.namespace}/${tableId.name}"

  /** Processes data synchronization by copying files between regions.
    */
  private def processDataSync(event: SyncEvent): IO[SyncError, Boolean] =
    for
      _ <- ZIO.logDebug(
        s"Processing data sync for table ${event.tableId} from ${event.sourceRegion.id} to ${event.targetRegion.id}")

      // Get storage locations for source and target regions
      sourceStorage <- storagePort
        .getStorageLocation(event.sourceRegion)
        .mapError(storageErrorToSyncError)
      targetStorage <- storagePort
        .getStorageLocation(event.targetRegion)
        .mapError(storageErrorToSyncError)

      // Get the table metadata to know which files to copy
      metadataOpt <- catalogPort
        .getMetadata(event.tableId, event.commitId)
        .mapError(catalogErrorToSyncError)
      metadata <- ZIO
        .fromOption(metadataOpt)
        .orElseFail(DomainError.SyncEventNotFound(event.eventId))

      targetBasePath <- registryPort
        .getTableDataPath(event.tableId, event.targetRegion)
        .mapError(storageErrorToSyncError)
        .someOrFail(DomainError.RegionNotAvailable(event.targetRegion))

      // Copy each data file in parallel
      copyResults <- ZIO.foreachPar(metadata.dataFiles) { dataFile =>
        val targetPath = StoragePath(s"$targetBasePath/${dataFile.fileName}")

        storagePort
          .fileExists(targetStorage, targetPath)
          .flatMap { exists =>
            if !exists then
              storagePort.copyFile(sourceStorage, dataFile, targetStorage, targetPath) *>
                ZIO.logDebug(s"Copied file $dataFile to $targetPath") *>
                ZIO.succeed(1)
            else ZIO.succeed(0)
          }
          .mapError(storageErrorToSyncError)
      }

      copiedFiles = copyResults.sum
      _ <- ZIO.logInfo(
        s"Copied $copiedFiles files for table ${event.tableId} from ${event.sourceRegion.id} to ${event.targetRegion.id}")
    yield true

  // Error mapping utilities
  private def catalogErrorToSyncError(error: CatalogError): SyncError =
    error match
      case DomainError.TableNotFound(tableId) =>
        DomainError.SyncEventNotFound(EventId.generate("table-not-found"))
      case _ =>
        DomainError.RegionNotAvailable(Region("unknown", "Unknown Region"))

  /** Processes commit completion notification.
    */
  private def processCommitCompleted(event: SyncEvent): IO[SyncError, Boolean] =
    ZIO.logDebug(s"Processing commit completed notification for ${event.eventId}") *>
      ZIO.succeed(true)

  /** Retries all failed events for a region.
    */
  def retryFailedEvents(region: Region): IO[SyncError, Int] =
    for
      _ <- ZIO.logInfo(s"Retrying failed sync events for region ${region.id}")
      failedEvents <- syncPort.getFailedEvents(region)
      retriedCount <- ZIO.foldLeft(failedEvents)(0) { (count, event) =>
        syncPort
          .retryFailedEvent(event.eventId)
          .as(count + 1)
          .tap(_ => ZIO.logDebug(s"Retried failed event ${event.eventId}"))
          .catchAll(error =>
            ZIO.logError(s"Failed to retry event ${event.eventId}: $error") *>
              ZIO.succeed(count))
      }
      _ <- ZIO.logInfo(s"Retried $retriedCount failed events for region ${region.id}")
    yield retriedCount

  private def storageErrorToSyncError(error: StorageError): SyncError =
    error match
      case DomainError.StorageLocationNotFound(region) =>
        DomainError.RegionNotAvailable(region)
      case _ =>
        DomainError.RegionNotAvailable(Region("unknown", "Unknown Region"))

object SyncOrchestrator:
  /** Creates a ZLayer for dependency injection
    */
  val live: ZLayer[SyncPort & StoragePort & CatalogPort & RegistryPort, Nothing, SyncOrchestrator] =
    ZLayer {
      for
        syncPort <- ZIO.service[SyncPort]
        storagePort <- ZIO.service[StoragePort]
        catalogPort <- ZIO.service[CatalogPort]
        registryPort <- ZIO.service[RegistryPort]
      yield SyncOrchestrator(syncPort, storagePort, catalogPort, registryPort)
    }
