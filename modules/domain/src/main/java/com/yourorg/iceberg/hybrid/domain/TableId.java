package com.yourorg.iceberg.hybrid.domain;

/**
 * Unique identifier for an Iceberg table within the hybrid replication system.
 * 
 * <p>This immutable record combines namespace and table name to create a fully qualified
 * table identifier that is consistent across both on-premise and cloud environments.
 * 
 * <p>Example usage:
 * <pre>{@code
 * var tableId = new TableId("warehouse", "orders");
 * // Represents table "warehouse.orders" in both SoT and mirror catalogs
 * }</pre>
 * 
 * @param namespace the database or schema namespace (e.g., "warehouse", "analytics")
 * @param name the table name within the namespace (e.g., "orders", "customers")
 */
public record TableId(String namespace, String name) {}
