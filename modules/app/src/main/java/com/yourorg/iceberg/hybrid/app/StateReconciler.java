package com.yourorg.iceberg.hybrid.app;

import com.yourorg.iceberg.hybrid.domain.*;
import com.yourorg.iceberg.hybrid.ports.*;

import java.time.Instant;

public final class StateReconciler {

  private final CatalogPort dstCatalog;
  private final ObjectStorePort objDst;

  public StateReconciler(CatalogPort dstCatalog, ObjectStorePort objDst) {
    this.dstCatalog = dstCatalog;
    this.objDst = objDst;
  }

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
