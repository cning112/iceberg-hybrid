package com.yourorg.iceberg.hybrid.domain;
import java.time.Instant;
public record FileRef(String path, ContentType contentType, String partition,
                      long size, String etag, Instant lastModified) {}
