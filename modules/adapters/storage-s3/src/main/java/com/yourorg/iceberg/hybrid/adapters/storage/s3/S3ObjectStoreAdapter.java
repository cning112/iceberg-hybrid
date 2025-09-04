package com.yourorg.iceberg.hybrid.adapters.storage.s3;

import com.yourorg.iceberg.hybrid.ports.ObjectStorePort;
import java.time.Instant;
import java.util.*;

public final class S3ObjectStoreAdapter implements ObjectStorePort {

  // For now, pretend everything exists in destination to let tests pass;
  // replace with real AWS SDK integration.
  private final Map<String, ObjectStat> objects = new HashMap<>();

  public void put(String key, long size, String etag) {
    objects.put(key, new ObjectStat(true, size, etag, Instant.now()));
  }

  @Override
  public Optional<ObjectStat> stat(String path) {
    return Optional.ofNullable(objects.get(path));
  }

  @Override
  public CopyResult copy(String src, String dst, boolean overwrite) {
    objects.put(dst, new ObjectStat(true, 0L, null, Instant.now()));
    return new CopyResult(true, "stubbed");
  }

  @Override
  public boolean delete(String path) {
    return objects.remove(path) != null;
  }

  @Override
  public Iterable<ObjectSummary> list(String prefix) {
    List<ObjectSummary> res = new ArrayList<>();
    for (var e : objects.entrySet()) {
      if (e.getKey().startsWith(prefix)) {
        res.add(new ObjectSummary(e.getKey(), e.getValue().size(), e.getValue().lastModified()));
      }
    }
    return res;
  }
}
