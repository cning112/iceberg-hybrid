package com.streamfirst.iceberg.hybrid.domain;

import java.util.Objects;

/**
 * Unique identifier for an Apache Iceberg table in the catalog.
 * Follows the standard namespace.table naming convention used by Iceberg catalogs.
 * 
 * @param namespace the table namespace (e.g., "analytics", "warehouse.sales")
 * @param name the table name within the namespace (e.g., "user_events", "transactions")
 */
public record TableId(String namespace, String name) {
    public TableId {
        Objects.requireNonNull(namespace, "Table namespace cannot be null");
        Objects.requireNonNull(name, "Table name cannot be null");
    }

    /**
     * Returns the fully qualified table name in the format "namespace.name".
     * This is the canonical representation used in catalog operations.
     */
    public String fullyQualifiedName() {
        return namespace + "." + name;
    }

    @Override
    public String toString() {
        return fullyQualifiedName();
    }
}