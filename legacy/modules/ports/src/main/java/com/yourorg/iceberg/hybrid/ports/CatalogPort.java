package com.yourorg.iceberg.hybrid.ports;

import com.yourorg.iceberg.hybrid.domain.*;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Port interface for Iceberg catalog operations in the hybrid replication system.
 * 
 * <p>This interface abstracts catalog implementations (such as Nessie, Hive, or BigLake REST)
 * and provides the core operations needed for both the Source-of-Truth (SoT) catalog and
 * the cloud mirror catalog.
 * 
 * <p>Key responsibilities include:
 * <ul>
 * <li>Managing table metadata and snapshot references</li>
 * <li>Atomic commit operations with optimistic concurrency control</li>
 * <li>Snapshot visibility control for coordinated promotion</li>
 * <li>Table-level locking for coordination between environments</li>
 * </ul>
 * 
 * <p>Implementations must ensure:
 * <ul>
 * <li>Thread safety for concurrent operations</li>
 * <li>Atomic commits with proper conflict detection</li>
 * <li>Consistent snapshot ordering via sequence numbers</li>
 * </ul>
 */
public interface CatalogPort {
  /**
   * Retrieves the current metadata for a table including all snapshots.
   * 
   * @param id the table identifier
   * @return table metadata with current snapshot references
   * @throws IllegalArgumentException if table does not exist
   */
  TableMetadata getTableMetadata(TableId id);

  /**
   * Lists all snapshots for a table in sequence order.
   * 
   * <p>Returns snapshots ordered by sequence number, which is critical for
   * replication planning and consistency management.
   * 
   * @param id the table identifier
   * @return list of snapshot references ordered by sequence number
   */
  List<SnapshotRef> listSnapshots(TableId id);

  /**
   * Atomically commits a new snapshot to the catalog.
   * 
   * <p>Uses optimistic concurrency control - if expectedParent is provided
   * and doesn't match the current HEAD, the commit will fail.
   * 
   * @param id the table identifier
   * @param snapshot the new snapshot to commit
   * @param expectedParent optional expected parent snapshot for conflict detection
   * @return result indicating success/failure with details
   */
  CommitResult commitSnapshot(TableId id, Snapshot snapshot, Optional<SnapshotId> expectedParent);

  /**
   * Controls visibility of a snapshot for coordinated promotion.
   * 
   * <p>Used by the cloud-side reconciler to make snapshots visible only after
   * verifying all referenced files are available in cloud storage.
   * 
   * @param id the table identifier
   * @param snapshotId the snapshot to make visible
   * @param visibleAt timestamp when snapshot becomes queryable
   */
  void setVisibility(TableId id, SnapshotId snapshotId, Instant visibleAt);

  /**
   * Acquires a distributed lock on a table for coordination.
   * 
   * @param id the table identifier
   * @param owner identifier for the lock holder
   * @param ttlSeconds time-to-live for the lock in seconds
   * @return true if lock was acquired, false if already held
   */
  boolean acquireTableLock(TableId id, String owner, long ttlSeconds);

  /**
   * Releases a previously acquired table lock.
   * 
   * @param id the table identifier
   * @param owner identifier for the lock holder (must match acquire call)
   */
  void releaseTableLock(TableId id, String owner);

  record TableMetadata(TableId id, List<SnapshotRef> snapshots) {}
  record SnapshotRef(SnapshotId id, List<Manifest> manifests) {}
  record Snapshot(SnapshotId id, List<Manifest> manifests, Map<String,String> props) {}
  record CommitResult(SnapshotId id, boolean success, String message) {}
}
