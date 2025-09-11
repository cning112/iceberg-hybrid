package com.streamfirst.iceberg.hybrid.ports;

import com.streamfirst.iceberg.hybrid.domain.*;

import java.util.List;
import java.util.function.Predicate;

/**
 * Port for managing inter-region synchronization events.
 * Coordinates the replication of metadata and data between geographic regions.
 */
public interface SyncPort {
    
    /**
     * Publishes a synchronization event to trigger replication.
     * 
     * @param event the sync event to publish
     * @throws RuntimeException if event cannot be published
     */
    void publishSyncEvent(SyncEvent event);
    
    /**
     * Gets sync events matching the specified criteria.
     * Provides flexible querying for complex synchronization scenarios.
     * 
     * @param predicate the filter criteria for sync events
     * @return list of matching sync events, ordered by creation time
     */
    List<SyncEvent> getSyncEvents(Predicate<SyncEvent> predicate);
    
    /**
     * Gets all pending sync events for a target region.
     * Used by regional sync workers to process their queue.
     * 
     * @param targetRegion the region to get pending events for
     * @return list of pending sync events, ordered by creation time
     */
    default List<SyncEvent> getPendingEvents(Region targetRegion) {
        return getSyncEvents(event -> event.getTargetRegion().equals(targetRegion) 
            && event.getStatus() == SyncEvent.Status.PENDING);
    }
    
    /**
     * Updates the status of a synchronization event.
     * 
     * @param eventId the event identifier
     * @param status the new status (IN_PROGRESS, COMPLETED, FAILED)
     * @throws IllegalArgumentException if event doesn't exist
     */
    void updateEventStatus(EventId eventId, SyncEvent.Status status);
    
    /**
     * Creates a metadata synchronization event.
     * This triggers replication of table metadata to the target region.
     * 
     * @param metadata the table metadata to sync
     * @param targetRegion the region to sync to
     * @return the created sync event
     */
    SyncEvent createMetadataSyncEvent(TableMetadata metadata, Region targetRegion);
    
    /**
     * Creates a data synchronization event.
     * This triggers replication of data files to the target region.
     * 
     * @param metadata the table metadata context
     * @param dataFiles the specific data files to sync
     * @param targetRegion the region to sync to
     * @return the created sync event
     */
    SyncEvent createDataSyncEvent(TableMetadata metadata, List<StoragePath> dataFiles, Region targetRegion);
    
    /**
     * Gets the synchronization history for a table in a specific region.
     * 
     * @param tableId the table identifier
     * @param region the region to get history for
     * @return list of sync events for the table in that region
     */
    default List<SyncEvent> getEventHistory(TableId tableId, Region region) {
        return getSyncEvents(event -> event.getTableId().equals(tableId) 
            && event.getTargetRegion().equals(region));
    }
    
    /**
     * Gets all failed synchronization events for a region.
     * Used for monitoring and manual intervention.
     * 
     * @param region the region to get failed events for
     * @return list of failed sync events
     */
    default List<SyncEvent> getFailedEvents(Region region) {
        return getSyncEvents(event -> event.getTargetRegion().equals(region) 
            && event.getStatus() == SyncEvent.Status.FAILED);
    }
    
    /**
     * Retries a failed synchronization event.
     * Resets the event status to PENDING for reprocessing.
     * 
     * @param eventId the event identifier to retry
     * @throws IllegalArgumentException if event doesn't exist or isn't failed
     */
    void retryFailedEvent(EventId eventId);
}