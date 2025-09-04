package com.yourorg.iceberg.hybrid.domain;
import java.time.Instant;
public record SnapshotId(String uuid, long sequenceNumber, Instant commitTs) {}
