package com.yourorg.iceberg.hybrid.app;

import com.yourorg.iceberg.hybrid.domain.*;
import com.yourorg.iceberg.hybrid.ports.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.Comparator;

/**
 * Application service responsible for calculating replication plans between environments.
 * 
 * <p>The ReplicationPlanner implements the core "snapshot diffing" strategy described in
 * the system design. It analyzes the difference between the current state of the 
 * Source-of-Truth (SoT) and the cloud mirror to determine exactly which files need
 * to be copied for replication.
 * 
 * <p>Key optimizations implemented:
 * <ul>
 * <li><strong>Incremental replication:</strong> Only copies new/changed files since last sync</li>
 * <li><strong>Inventory-based deduplication:</strong> Uses cloud inventory to avoid unnecessary transfers</li>
 * <li><strong>ETag verification:</strong> Validates file integrity to prevent corrupted replicas</li>
 * <li><strong>Size checking:</strong> Additional verification layer for data consistency</li>
 * </ul>
 * 
 * <p>The planning algorithm:
 * <ol>
 * <li>Identifies the latest snapshot in the destination (cloud) catalog</li>
 * <li>Compares manifests between source snapshot and destination to find new files</li>
 * <li>Filters out files already present in cloud storage using inventory and stat checks</li>
 * <li>Returns a minimal set of objects that require copying</li>
 * </ol>
 * 
 * @see com.yourorg.iceberg.hybrid.app.StateReconciler for the promotion phase
 */
public final class ReplicationPlanner {

  private final CatalogPort srcCatalog;
  private final CatalogPort dstCatalog;
  private final InventoryPort inventory;
  private final ObjectStorePort objDst;

  public ReplicationPlanner(CatalogPort srcCatalog,
                            CatalogPort dstCatalog,
                            InventoryPort inventory,
                            ObjectStorePort objDst) {
    this.srcCatalog = srcCatalog;
    this.dstCatalog = dstCatalog;
    this.inventory = inventory;
    this.objDst = objDst;
  }

  /**
   * Creates a replication plan for syncing a specific snapshot to the cloud mirror.
   * 
   * <p>This method implements the core diffing algorithm that determines the minimal
   * set of files needed to replicate a snapshot from the source to destination catalog.
   * 
   * <p>The algorithm performs three levels of deduplication:
   * <ol>
   * <li>Manifest-level: Compare manifests between source and destination snapshots</li>
   * <li>Inventory-level: Check cloud inventory for existing files</li>
   * <li>Object-level: Verify individual file existence and integrity via stat calls</li>
   * </ol>
   * 
   * @param tableId the table being replicated
   * @param snapshotId the specific snapshot to replicate from source
   * @param idx cloud inventory index for efficient file existence checking
   * @return plan containing list of object URIs that need to be copied
   */
  public PlanResult plan(TableId tableId, SnapshotId snapshotId, InventoryPort.InventoryIndex idx) {
    var toSnapshot = srcCatalog.listSnapshots(tableId).stream()
        .filter(s -> s.id().equals(snapshotId))
        .findFirst().orElseThrow();

    var fromSnapshot = dstCatalog.listSnapshots(tableId).stream()
        .max(Comparator.comparing(s -> s.id().sequenceNumber()));

    var fromManifests = fromSnapshot.map(s -> s.manifests().stream().map(Manifest::path).collect(Collectors.toSet()))
        .orElse(Collections.emptySet());

    var newFiles = toSnapshot.manifests().stream()
        .filter(m -> !fromManifests.contains(m.path()))
        .flatMap(m -> m.files().stream())
        .collect(Collectors.toList());

    List<FileRef> toCopy = new ArrayList<>();
    for (var f : newFiles) {
      boolean exists = inventory.contains(idx, f.path(), f.etag(), f.size());
      if (!exists) {
        var st = objDst.stat(f.path());
        if (st.isEmpty() || !st.get().exists()
            || st.get().size() != f.size()
            || (st.get().etag() != null && f.etag() != null && !st.get().etag().equals(f.etag()))) {
          toCopy.add(f);
        }
      }
    }
    return new PlanResult(toCopy.stream().map(FileRef::path).toList());
  }

  public record PlanResult(List<String> objectsToCopy) {}
}
