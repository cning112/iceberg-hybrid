package com.yourorg.iceberg.hybrid.ports;

import java.time.Instant;
import java.util.Optional;

/**
 * Port interface for object storage operations in the hybrid replication system.
 * 
 * <p>Abstracts cloud object storage services (S3, GCS, Azure Blob) and provides
 * the essential operations needed for Iceberg data file management and replication.
 * 
 * <p>This interface is used by:
 * <ul>
 * <li>{@link com.yourorg.iceberg.hybrid.app.ReplicationPlanner} - to check file existence during planning</li>
 * <li>{@link com.yourorg.iceberg.hybrid.app.StateReconciler} - to verify files before promotion</li>
 * <li>{@link com.yourorg.iceberg.hybrid.app.GCCoordinator} - to safely delete expired files</li>
 * </ul>
 * 
 * <p>Implementations should provide:
 * <ul>
 * <li>Efficient batch operations where possible</li>
 * <li>Consistent error handling and retry mechanisms</li>
 * <li>Proper ETag support for integrity verification</li>
 * </ul>
 */
public interface ObjectStorePort {
  /**
   * Retrieves metadata about an object without downloading it.
   * 
   * <p>This operation is used extensively during replication planning and verification
   * to check file existence, size, and integrity without transferring data.
   * 
   * @param path the full object URI (e.g., "s3://bucket/path/file.parquet")
   * @return object metadata if it exists, empty if not found
   */
  Optional<ObjectStat> stat(String path);

  /**
   * Copies an object from source to destination location.
   * 
   * <p>Implementations should use server-side copy operations when possible
   * to minimize network traffic and improve performance.
   * 
   * @param src source object URI
   * @param dst destination object URI
   * @param overwrite whether to overwrite if destination exists
   * @return result indicating success/failure with details
   */
  CopyResult copy(String src, String dst, boolean overwrite);

  /**
   * Deletes an object from storage.
   * 
   * <p>Used by the GC coordinator to remove expired files. Implementations
   * should be idempotent (deleting non-existent objects should succeed).
   * 
   * @param path the object URI to delete
   * @return true if deleted (or didn't exist), false if deletion failed
   */
  boolean delete(String path);

  /**
   * Lists objects with a given prefix.
   * 
   * <p>Used for inventory operations and bulk file discovery. Results should
   * be returned in a memory-efficient streaming manner for large datasets.
   * 
   * @param prefix the object key prefix to filter by
   * @return iterable of object summaries matching the prefix
   */
  Iterable<ObjectSummary> list(String prefix);

  record ObjectStat(boolean exists, long size, String etag, Instant lastModified) {}
  record ObjectSummary(String key, long size, Instant lastModified) {}
  record CopyResult(boolean success, String message) {}
}
