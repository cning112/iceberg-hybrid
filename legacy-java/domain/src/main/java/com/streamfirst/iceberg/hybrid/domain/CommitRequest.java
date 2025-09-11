package com.streamfirst.iceberg.hybrid.domain;

import java.time.Instant;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;

/**
 * Represents a request to commit new metadata to the global catalog. Contains all changes that need
 * to be atomically applied to create a new table version. Must be approved by all required regions
 * before committing.
 */
@Value
@EqualsAndHashCode(of = {"tableId", "sourceRegion", "requestTime"})
public class CommitRequest {
  /** The table being modified */
  @NonNull TableId tableId;

  /** The region where the write operation originated */
  @NonNull Region sourceRegion;

  /** When this commit request was created */
  @NonNull Instant requestTime;

  /** New data files being added in this commit */
  @NonNull List<StoragePath> newDataFiles;

  /** The updated table schema (may be unchanged from previous version) */
  @NonNull String updatedSchema;

  /** Description of the operation (e.g., "INSERT", "UPDATE", "DELETE", "SCHEMA_CHANGE") */
  @NonNull String operation;

  @Override
  public String toString() {
    return "CommitRequest{"
        + "tableId="
        + tableId
        + ", sourceRegion="
        + sourceRegion
        + ", operation='"
        + operation
        + '\''
        + ", newDataFileCount="
        + newDataFiles.size()
        + '}';
  }
}
