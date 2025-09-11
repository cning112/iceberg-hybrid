package com.streamfirst.iceberg.hybrid.domain;

import java.util.Objects;

/**
 * Unique identifier for a table metadata commit in the global catalog. Each commit represents a
 * specific version of table metadata (schema, partitioning, etc.). Commit IDs are globally unique
 * and monotonically increasing per table.
 *
 * @param value the commit identifier (typically a UUID or timestamp-based ID)
 */
public record CommitId(String value) {
  public CommitId {
    Objects.requireNonNull(value, "Commit ID cannot be null");
    if (value.trim().isEmpty()) {
      throw new IllegalArgumentException("Commit ID cannot be empty");
    }
  }

  @Override
  public String toString() {
    return value;
  }
}
