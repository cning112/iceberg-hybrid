package com.streamfirst.iceberg.hybrid.domain;

import java.util.Objects;

/**
 * Represents a geographic region in the distributed Iceberg system. Each region has its own
 * storage, compute, and replication capabilities. Regions participate in distributed consensus for
 * write operations.
 *
 * @param id unique identifier for the region (e.g., "us-east-1", "eu-west-1")
 * @param displayName human-readable name for the region (e.g., "US East", "Europe West")
 */
public record Region(String id, String displayName) {
  public Region {
    Objects.requireNonNull(id, "Region id cannot be null");
    Objects.requireNonNull(displayName, "Region displayName cannot be null");
  }
}
