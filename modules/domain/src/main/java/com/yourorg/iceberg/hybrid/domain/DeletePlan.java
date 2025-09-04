package com.yourorg.iceberg.hybrid.domain;
import java.time.Instant;
import java.util.List;
public record DeletePlan(TableId tableId, List<String> deleteCandidates, Instant generatedAt,
                         Instant validFrom, Instant validUntil, java.util.List<String> approvals) {}
