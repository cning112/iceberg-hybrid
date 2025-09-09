package com.streamfirst.iceberg.hybrid.domain;
import java.time.Instant;
public record RetentionPolicy(int minSnapshotsToKeep, long maxSnapshotAgeSeconds, Instant minCleanTs) {}
