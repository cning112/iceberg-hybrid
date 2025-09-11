package com.streamfirst.iceberg.hybrid.domain;

import java.time.Instant;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;

/**
 * Represents a specific version of Apache Iceberg table metadata. Contains all information needed
 * to describe the table's structure and data files at a particular point in time. Metadata is
 * versioned by commit ID and replicated across regions for geo-distributed access.
 */
@Value
@EqualsAndHashCode(of = {"tableId", "commitId"})
public class TableMetadata {
  /** The table this metadata describes */
  @NonNull TableId tableId;

  /** Unique identifier for this metadata version */
  @NonNull CommitId commitId;

  /** The region where this metadata version was originally created */
  @NonNull Region sourceRegion;

  /** When this metadata version was created */
  @NonNull Instant timestamp;

  /** List of data file paths that comprise the table at this version */
  @NonNull List<StoragePath> dataFiles;

  /** The table schema as JSON (Iceberg schema format) */
  @NonNull String schema;

  @Override
  public String toString() {
    return "TableMetadata{"
        + "tableId="
        + tableId
        + ", commitId="
        + commitId
        + ", sourceRegion="
        + sourceRegion
        + ", timestamp="
        + timestamp
        + ", dataFileCount="
        + dataFiles.size()
        + '}';
  }
}
