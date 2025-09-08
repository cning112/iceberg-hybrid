package com.yourorg.iceberg.hybrid.ports;

public interface InventoryPort {
  InventoryIndex loadIndex(String bucket, String prefix, String asOfDate);
  boolean contains(InventoryIndex index, String key, String etag, long size);

  interface InventoryIndex { String version(); }
}
