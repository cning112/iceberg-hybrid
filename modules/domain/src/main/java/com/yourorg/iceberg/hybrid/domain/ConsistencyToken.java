package com.yourorg.iceberg.hybrid.domain;

import java.time.Instant;

/**
 * Token representing the consistency state of a table in the cloud mirror.
 * 
 * <p>This token tracks the replication progress and enables the {@link com.yourorg.iceberg.hybrid.app.ReadRouter}
 * to determine whether a requested snapshot is available in the cloud mirror or must be 
 * served from the on-premise Source-of-Truth (SoT).
 * 
 * <p>The token is updated by the replication system after successful promotion of snapshots
 * to the cloud catalog, ensuring that read routing decisions are based on actual data availability.
 * 
 * <p>Components of consistency tracking:
 * <ul>
 * <li><strong>High watermark:</strong> Latest timestamp of data visible in cloud</li>
 * <li><strong>Sequence tracking:</strong> Monotonic counter for ordering validation</li>
 * <li><strong>Inventory version:</strong> Correlation with object store inventory state</li>
 * </ul>
 * 
 * @param highWatermarkTs latest commit timestamp of data available in the cloud mirror
 * @param lastAppliedSequence highest sequence number successfully replicated to cloud
 * @param inventoryVersion version identifier for object store inventory correlation
 */
public record ConsistencyToken(Instant highWatermarkTs, long lastAppliedSequence, String inventoryVersion) {}
