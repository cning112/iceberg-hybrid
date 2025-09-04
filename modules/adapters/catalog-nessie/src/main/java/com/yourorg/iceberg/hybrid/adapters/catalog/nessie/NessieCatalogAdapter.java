package com.yourorg.iceberg.hybrid.adapters.catalog.nessie;

import com.yourorg.iceberg.hybrid.domain.*;
import com.yourorg.iceberg.hybrid.ports.CatalogPort;
import java.time.Instant;
import java.util.*;

public final class NessieCatalogAdapter implements CatalogPort {

  // In-memory stub storage for now
  private final Map<TableId, TableMetadata> meta = new HashMap<>();

  @Override
  public TableMetadata getTableMetadata(TableId id) {
    return meta.getOrDefault(id, new TableMetadata(id, List.of()));
  }

  @Override
  public List<CatalogPort.SnapshotRef> listSnapshots(TableId id) {
    return meta.getOrDefault(id, new TableMetadata(id, List.of())).snapshots();
  }

  @Override
  public CommitResult commitSnapshot(TableId id, Snapshot snapshot, Optional<SnapshotId> expectedParent) {
    var cur = meta.getOrDefault(id, new TableMetadata(id, new ArrayList<>()));
    var list = new ArrayList<>(cur.snapshots());
    list.add(new SnapshotRef(snapshot.id(), snapshot.manifests()));
    var newMeta = new TableMetadata(id, List.copyOf(list));
    meta.put(id, newMeta);
    return new CommitResult(snapshot.id(), true, "OK");
  }

  @Override
  public void setVisibility(TableId id, SnapshotId snapshotId, Instant visibleAt) {
    // For stub, no-op
  }

  @Override
  public boolean acquireTableLock(TableId id, String owner, long ttlSeconds) { return true; }

  @Override
  public void releaseTableLock(TableId id, String owner) {}

}
