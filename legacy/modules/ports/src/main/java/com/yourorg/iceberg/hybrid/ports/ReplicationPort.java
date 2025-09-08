package com.yourorg.iceberg.hybrid.ports;

import com.yourorg.iceberg.hybrid.domain.*;
import java.util.List;
import java.util.Optional;

public interface ReplicationPort {
  String enqueue(ReplicationJob job);
  Optional<ReplicationJob> get(String jobId);
  void update(String jobId, ReplicationJob.Status status, String stage, ReplicationMetrics m);

  record ReplicationJob(String jobId, TableId tableId, SnapshotId srcSnapshot,
                        List<String> objectsToCopy, Status status) {
    public enum Status { PENDING, RUNNING, DONE, FAILED }
  }
  record ReplicationMetrics(long bytesCopied, int filesCopied, long millis) {}
}
