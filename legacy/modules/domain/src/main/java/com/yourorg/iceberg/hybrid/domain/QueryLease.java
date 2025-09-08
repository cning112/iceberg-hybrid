package com.yourorg.iceberg.hybrid.domain;
import java.time.Instant;
public record QueryLease(String leaseId, TableId tableId, SnapshotId snapshotId, String holder, Instant expireAt) {}
