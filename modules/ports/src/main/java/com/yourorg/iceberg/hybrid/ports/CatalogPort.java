package com.yourorg.iceberg.hybrid.ports;

import com.yourorg.iceberg.hybrid.domain.*;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface CatalogPort {
  TableMetadata getTableMetadata(TableId id);
  List<SnapshotRef> listSnapshots(TableId id);
  CommitResult commitSnapshot(TableId id, Snapshot snapshot, Optional<SnapshotId> expectedParent);
  void setVisibility(TableId id, SnapshotId snapshotId, Instant visibleAt);
  boolean acquireTableLock(TableId id, String owner, long ttlSeconds);
  void releaseTableLock(TableId id, String owner);

  record TableMetadata(TableId id, List<SnapshotRef> snapshots) {}
  record SnapshotRef(SnapshotId id, List<Manifest> manifests) {}
  record Snapshot(SnapshotId id, List<Manifest> manifests, Map<String,String> props) {}
  record CommitResult(SnapshotId id, boolean success, String message) {}
}
