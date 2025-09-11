package com.streamfirst.iceberg.hybrid.application;

import com.streamfirst.iceberg.hybrid.domain.*;
import com.streamfirst.iceberg.hybrid.ports.*;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Orchestrates synchronization operations between regions. Processes sync events and coordinates
 * data/metadata replication. Handles both metadata synchronization and data file copying.
 */
@Slf4j
@RequiredArgsConstructor
public class SyncOrchestrator {

  private final SyncPort syncPort;
  private final StoragePort storagePort;
  private final CatalogPort catalogPort;
  private final RegistryPort registryPort;

  /**
   * Processes all pending sync events for a specific region. This method is typically called by a
   * scheduled worker process.
   *
   * @param region the region to process events for
   * @return number of events processed successfully
   */
  public int processPendingEvents(Region region) {
    log.debug("Processing pending sync events for region {}", region);

    List<SyncEvent> pendingEvents = syncPort.getPendingEvents(region);
    int successCount = 0;

    for (SyncEvent event : pendingEvents) {
      try {
        boolean success = processSyncEvent(event);
        if (success) {
          successCount++;
        }
      } catch (Exception e) {
        log.error("Failed to process sync event {}", event.getEventId(), e);
        updateEventStatus(event.getEventId(), SyncEvent.Status.FAILED);
      }
    }

    log.info(
        "Processed {} out of {} pending events for region {}",
        successCount,
        pendingEvents.size(),
        region);
    return successCount;
  }

  /** Processes a single synchronization event based on its type. */
  private boolean processSyncEvent(SyncEvent event) {
    log.debug("Processing sync event: {}", event);

    updateEventStatus(event.getEventId(), SyncEvent.Status.IN_PROGRESS);

    try {
      boolean success =
          switch (event.getType()) {
            case METADATA_SYNC -> processMetadataSync(event);
            case DATA_SYNC -> processDataSync(event);
            case COMMIT_COMPLETED -> processCommitCompleted(event);
          };

      if (success) {
        updateEventStatus(event.getEventId(), SyncEvent.Status.COMPLETED);
        log.debug("Successfully processed sync event {}", event.getEventId());
      } else {
        updateEventStatus(event.getEventId(), SyncEvent.Status.FAILED);
        log.warn("Failed to process sync event {}", event.getEventId());
      }

      return success;
    } catch (Exception e) {
      updateEventStatus(event.getEventId(), SyncEvent.Status.FAILED);
      log.error("Exception while processing sync event {}", event.getEventId(), e);
      return false;
    }
  }

  /** Processes metadata synchronization by fetching and storing metadata locally. */
  private boolean processMetadataSync(SyncEvent event) {
    log.debug(
        "Processing metadata sync for table {} to region {}",
        event.getTableId(),
        event.getTargetRegion());

    try {
      // Fetch the latest metadata for the commit
      var metadataOpt = catalogPort.getMetadata(event.getTableId(), event.getCommitId());

      if (metadataOpt.isEmpty()) {
        log.warn(
            "Metadata not found for table {} commit {}", event.getTableId(), event.getCommitId());
        return false;
      }

      // Register table location in target region if not already present
      var existingPath = registryPort.getTableDataPath(event.getTableId(), event.getTargetRegion());
      if (existingPath.isEmpty()) {
        String dataPath = generateTableDataPath(event.getTableId());
        registryPort.registerTableLocation(event.getTableId(), event.getTargetRegion(), dataPath);
        log.debug(
            "Registered table location for {} in region {}",
            event.getTableId(),
            event.getTargetRegion());
      }

      return true;
    } catch (Exception e) {
      log.error("Failed to process metadata sync for {}", event, e);
      return false;
    }
  }

  /** Processes data synchronization by copying files between regions. */
  private boolean processDataSync(SyncEvent event) {
    log.debug(
        "Processing data sync for table {} from {} to {}",
        event.getTableId(),
        event.getSourceRegion(),
        event.getTargetRegion());

    try {
      // Get storage locations for source and target regions
      StorageLocation sourceStorage = storagePort.getStorageLocation(event.getSourceRegion());
      StorageLocation targetStorage = storagePort.getStorageLocation(event.getTargetRegion());

      // Get the table metadata to know which files to copy
      var metadataOpt = catalogPort.getMetadata(event.getTableId(), event.getCommitId());
      if (metadataOpt.isEmpty()) {
        log.warn(
            "Metadata not found for table {} commit {}", event.getTableId(), event.getCommitId());
        return false;
      }

      TableMetadata metadata = metadataOpt.get();
      String targetBasePath =
          registryPort
              .getTableDataPath(event.getTableId(), event.getTargetRegion())
              .orElseThrow(() -> new IllegalStateException("No data path registered for table"));

      // Copy each data file
      int copiedFiles = 0;
      for (StoragePath dataFile : metadata.getDataFiles()) {
        try {
          StoragePath targetPath =
              StoragePath.of(targetBasePath + "/" + extractFileName(dataFile.path()));

          // Only copy if file doesn't already exist
          if (!storagePort.fileExists(targetStorage, targetPath)) {
            storagePort.copyFile(sourceStorage, dataFile, targetStorage, targetPath);
            copiedFiles++;
            log.debug("Copied file {} to {}", dataFile, targetPath);
          }
        } catch (Exception e) {
          log.error("Failed to copy file {} for table {}", dataFile, event.getTableId(), e);
          return false;
        }
      }

      log.info(
          "Copied {} files for table {} from {} to {}",
          copiedFiles,
          event.getTableId(),
          event.getSourceRegion(),
          event.getTargetRegion());
      return true;

    } catch (Exception e) {
      log.error("Failed to process data sync for {}", event, e);
      return false;
    }
  }

  /** Processes commit completion notification. */
  private boolean processCommitCompleted(SyncEvent event) {
    log.debug("Processing commit completed notification for {}", event);
    return true;
  }

  /** Retries all failed events for a region. */
  public int retryFailedEvents(Region region) {
    log.info("Retrying failed sync events for region {}", region);

    List<SyncEvent> failedEvents = syncPort.getFailedEvents(region);
    int retriedCount = 0;

    for (SyncEvent event : failedEvents) {
      try {
        syncPort.retryFailedEvent(event.getEventId());
        retriedCount++;
        log.debug("Retried failed event {}", event.getEventId());
      } catch (Exception e) {
        log.error("Failed to retry event {}", event.getEventId(), e);
      }
    }

    log.info("Retried {} failed events for region {}", retriedCount, region);
    return retriedCount;
  }

  /** Updates the status of a sync event with current timestamp. */
  private void updateEventStatus(EventId eventId, SyncEvent.Status status) {
    try {
      syncPort.updateEventStatus(eventId, status);
    } catch (Exception e) {
      log.error("Failed to update status for event {} to {}", eventId, status, e);
    }
  }

  /** Generates a standard data path for table data in a region. */
  private String generateTableDataPath(TableId tableId) {
    return String.format("tables/%s/%s", tableId.namespace(), tableId.name());
  }

  /** Extracts filename from a full file path. */
  private String extractFileName(String filePath) {
    int lastSlash = filePath.lastIndexOf('/');
    return lastSlash >= 0 ? filePath.substring(lastSlash + 1) : filePath;
  }
}
