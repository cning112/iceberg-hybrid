package com.streamfirst.iceberg.hybrid.domain;

import java.time.Instant;
import lombok.NonNull;

/**
 * Records a single approval or rejection from a region. Simple, orthogonal design - only tracks
 * what happened, not business rules.
 */
public record CommitApproval(
    @NonNull CommitRequest request,
    @NonNull Region region,
    @NonNull ApprovalStatus status,
    @NonNull Instant timestamp,
    String reason // optional reason for rejection
    ) {

  /** Creates an approval record. */
  public static CommitApproval approved(CommitRequest request, Region region) {
    return new CommitApproval(request, region, ApprovalStatus.APPROVED, Instant.now(), null);
  }

  /** Creates a rejection record. */
  public static CommitApproval rejected(CommitRequest request, Region region, String reason) {
    return new CommitApproval(request, region, ApprovalStatus.REJECTED, Instant.now(), reason);
  }

  /** Creates a pending record. */
  public static CommitApproval pending(CommitRequest request, Region region) {
    return new CommitApproval(request, region, ApprovalStatus.PENDING, Instant.now(), null);
  }

  public enum ApprovalStatus {
    APPROVED,
    REJECTED,
    PENDING
  }
}
