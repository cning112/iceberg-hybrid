package com.yourorg.iceberg.hybrid.app;

import com.yourorg.iceberg.hybrid.domain.*;
import com.yourorg.iceberg.hybrid.ports.*;
import java.util.*;
import java.util.stream.Collectors;

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

  public PlanResult plan(TableId tableId, SnapshotId snapshotId, InventoryPort.InventoryIndex idx) {
    var srcSnapshot = srcCatalog.listSnapshots(tableId).stream()
        .filter(s -> s.id().equals(snapshotId))
        .findFirst().orElseThrow();

    var allFiles = srcSnapshot.manifests().stream()
        .flatMap(m -> m.files().stream())
        .collect(Collectors.toList());

    List<FileRef> toCopy = new ArrayList<>();
    for (var f : allFiles) {
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
