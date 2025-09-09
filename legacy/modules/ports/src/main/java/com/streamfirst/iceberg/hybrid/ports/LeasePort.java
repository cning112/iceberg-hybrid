package com.streamfirst.iceberg.hybrid.ports;

import com.streamfirst.iceberg.hybrid.domain.*;
import java.util.List;

public interface LeasePort {
  String create(TableId tableId, SnapshotId snapshotId, String holder, long ttlSeconds);
  void renew(String leaseId, long ttlSeconds);
  void release(String leaseId);
  List<QueryLease> listActive(TableId tableId);
}
