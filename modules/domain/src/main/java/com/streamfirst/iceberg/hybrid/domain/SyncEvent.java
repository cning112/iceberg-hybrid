package com.streamfirst.iceberg.hybrid.domain;

import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import lombok.With;

import java.time.Instant;

/**
 * Represents a synchronization event for replicating data or metadata between regions.
 * Events track the progress of cross-region replication and provide audit trails
 * for debugging distributed synchronization issues.
 */
@Value
@EqualsAndHashCode(of = "eventId")
public class SyncEvent {
    /**
     * Types of synchronization events in the geo-distributed system.
     */
    public enum Type {
        /** Replicate table metadata to another region */
        METADATA_SYNC, 
        /** Replicate data files to another region */
        DATA_SYNC, 
        /** Notify that a commit has been completed globally */
        COMMIT_COMPLETED
    }

    /**
     * Current status of the synchronization event.
     */
    public enum Status {
        /** Event created but not yet processed */
        PENDING, 
        /** Event is currently being processed */
        IN_PROGRESS, 
        /** Event completed successfully */
        COMPLETED, 
        /** Event failed and requires intervention */
        FAILED
    }

    /** Unique identifier for this sync event */
    @NonNull EventId eventId;
    
    /** The type of synchronization being performed */
    @NonNull Type type;
    
    /** The table being synchronized */
    @NonNull TableId tableId;
    
    /** The specific commit/version being synchronized */
    @NonNull CommitId commitId;
    
    /** The region that has the data to be synchronized */
    @NonNull Region sourceRegion;
    
    /** The region that needs to receive the synchronized data */
    @NonNull Region targetRegion;
    
    /** Current processing status of this event */
    @NonNull @With Status status;
    
    /** When this event was originally created */
    @NonNull Instant createdAt;
    
    /** When this event was last updated */
    @NonNull @With Instant updatedAt;

    /**
     * Creates a copy of this event with updated status and timestamp.
     * Convenience method for status transitions.
     */
    public SyncEvent withStatus(Status newStatus, Instant updatedAt) {
        return this.withStatus(newStatus).withUpdatedAt(updatedAt);
    }

    @Override
    public String toString() {
        return "SyncEvent{" +
               "eventId='" + eventId + '\'' +
               ", type=" + type +
               ", tableId=" + tableId +
               ", sourceRegion=" + sourceRegion +
               ", targetRegion=" + targetRegion +
               ", status=" + status +
               '}';
    }
}