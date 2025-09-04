package com.yourorg.iceberg.hybrid.adapters.inventory.s3;

import com.yourorg.iceberg.hybrid.ports.InventoryPort;
import java.util.HashSet;
import java.util.Set;

public final class S3InventoryAdapter implements InventoryPort {

  public static final class Index implements InventoryIndex {
    private final String version;
    private final Set<String> keys = new HashSet<>();
    public Index(String version) { this.version = version; }
    public void add(String key) { keys.add(key); }
    @Override public String version() { return version; }
    public boolean has(String key) { return keys.contains(key); }
  }

  @Override
  public InventoryIndex loadIndex(String bucket, String prefix, String asOfDate) {
    // Stub: empty index
    return new Index("v0");
  }

  @Override
  public boolean contains(InventoryIndex idx, String key, String etag, long size) {
    if (idx instanceof Index index) {
      return index.has(key);
    }
    return false;
  }
}
