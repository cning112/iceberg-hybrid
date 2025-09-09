package com.streamfirst.iceberg.hybrid.domain;

import java.util.Objects;

/**
 * Represents a storage endpoint in a specific region.
 * Each region can have multiple storage locations for different purposes
 * (primary data, backup, temporary files, etc.).
 * 
 * @param region the geographic region where this storage is located
 * @param uri the storage URI (e.g., "s3://bucket-name", "hdfs://cluster/path")
 * @param type the storage type/purpose (e.g., "primary", "backup", "temp")
 */
public record StorageLocation(Region region, String uri, String type) {
    public StorageLocation {
        Objects.requireNonNull(region, "Region cannot be null");
        Objects.requireNonNull(uri, "URI cannot be null");
        Objects.requireNonNull(type, "Type cannot be null");
        
        if (uri.trim().isEmpty()) {
            throw new IllegalArgumentException("URI cannot be empty");
        }
    }
}