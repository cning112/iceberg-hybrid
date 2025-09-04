package com.yourorg.iceberg.hybrid.app;

import com.yourorg.iceberg.hybrid.domain.*;
import com.yourorg.iceberg.hybrid.ports.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.Comparator;

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
