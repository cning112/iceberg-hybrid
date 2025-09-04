package com.yourorg.iceberg.hybrid.domain;
import java.time.Instant;
public record ConsistencyToken(Instant highWatermarkTs, long lastAppliedSequence, String inventoryVersion) {}
