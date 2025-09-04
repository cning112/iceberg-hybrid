package com.yourorg.iceberg.hybrid.ports;

import java.time.Instant;
import java.util.Optional;

public interface ObjectStorePort {
  Optional<ObjectStat> stat(String path);
  CopyResult copy(String src, String dst, boolean overwrite);
  boolean delete(String path);
  Iterable<ObjectSummary> list(String prefix);

  record ObjectStat(boolean exists, long size, String etag, Instant lastModified) {}
  record ObjectSummary(String key, long size, Instant lastModified) {}
  record CopyResult(boolean success, String message) {}
}
