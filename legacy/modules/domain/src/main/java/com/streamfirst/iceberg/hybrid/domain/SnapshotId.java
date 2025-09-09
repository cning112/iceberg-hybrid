package com.streamfirst.iceberg.hybrid.domain;

import java.time.Instant;

/**
 * Unique identifier for an Iceberg table snapshot in the hybrid replication system.
 * 
 * <p>Combines multiple identifiers to ensure global uniqueness and enable proper ordering
 * of snapshots across the replication pipeline. This is used to track snapshot versions
 * from the Source-of-Truth (SoT) through to the cloud mirror.
 * 
 * <p>The sequence number provides a monotonically increasing order that helps with:
 * <ul>
 * <li>Determining the "latest" snapshot when multiple snapshots exist</li>
 * <li>Calculating snapshot differences for replication planning</li>
 * <li>Implementing fast-forward replication strategies</li>
 * </ul>
 * 
 * @param uuid unique string identifier for this snapshot (typically UUID format)
 * @param sequenceNumber monotonically increasing number for ordering snapshots
 * @param commitTs timestamp when this snapshot was committed to the SoT catalog
 */
public record SnapshotId(String uuid, long sequenceNumber, Instant commitTs) {}
