package com.streamfirst.iceberg.hybrid.app;

import com.streamfirst.iceberg.hybrid.domain.*;
import com.streamfirst.iceberg.hybrid.ports.*;

import java.time.Instant;

/**
 * Application service responsible for verifying and promoting snapshots in the cloud mirror.
 * 
 * <p>The StateReconciler implements the second phase of the two-phase replication process.
 * After files have been copied to cloud storage, this service performs verification and
 * atomically promotes the snapshot to be visible for queries.
 * 
 * <p>This corresponds to the "verify and promote" step in the system design, which ensures
 * that cloud queries never see incomplete or corrupted data by validating all referenced
 * files are present and correctly sized before making the snapshot queryable.
 * 
 * <p>Key safety guarantees:
 * <ul>
 * <li><strong>Atomicity:</strong> Snapshots are either fully visible or not visible at all</li>
 * <li><strong>Consistency:</strong> All referenced files are verified before promotion</li>
 * <li><strong>Durability:</strong> Only promotes after confirming cloud storage durability</li>
 * </ul>
 * 
 * <p>Verification process:
 * <ol>
 * <li>Retrieves snapshot metadata from destination catalog</li>
 * <li>Iterates through all manifests and their referenced files</li>
 * <li>Verifies each file exists in cloud storage with correct size</li>
 * <li>Atomically sets snapshot visibility timestamp if all checks pass</li>
 * </ol>
 * 
 * @see com.streamfirst.iceberg.hybrid.app.ReplicationPlanner for the planning phase
 */
public final class StateReconciler {

  private final CatalogPort dstCatalog;
  private final ObjectStorePort objDst;

  public StateReconciler(CatalogPort dstCatalog, ObjectStorePort objDst) {
    this.dstCatalog = dstCatalog;
    this.objDst = objDst;
  }

  /**
   * Verifies all files for a snapshot exist in cloud storage and promotes it for querying.
   * 
   * <p>This method implements the critical "verify and promote" operation that ensures
   * cloud queries never encounter missing or corrupted files. It performs comprehensive
   * validation before making the snapshot visible.
   * 
   * <p>The verification includes:
   * <ul>
   * <li>Existence check for every referenced file</li>
   * <li>Size validation to detect truncated transfers</li>
   * <li>Atomic promotion to prevent partial visibility</li>
   * </ul>
   * 
   * @param tableId the table containing the snapshot to promote
   * @param snapshotId the snapshot identifier to verify and promote  
   * @param visibleAt timestamp when the snapshot should become queryable
   * @throws IllegalStateException if any file is missing or corrupted
   */
  public void verifyAndPromote(TableId tableId, SnapshotId snapshotId, Instant visibleAt) {
    var snap = dstCatalog.listSnapshots(tableId).stream()
        .filter(s -> s.id().equals(snapshotId))
        .findFirst().orElseThrow();

    for (var m : snap.manifests()) {
      for (var f : m.files()) {
        var st = objDst.stat(f.path())
                       .orElseThrow(() -> new IllegalStateException("Missing: " + f.path()));
        if (!st.exists() || st.size() != f.size()) {
          throw new IllegalStateException("Corrupt or size mismatch: " + f.path());
        }
      }
    }
    dstCatalog.setVisibility(tableId, snapshotId, visibleAt);
  }
}
