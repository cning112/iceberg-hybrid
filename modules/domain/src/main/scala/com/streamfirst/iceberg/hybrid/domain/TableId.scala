package com.streamfirst.iceberg.hybrid.domain

/**
 * Unique identifier for an Apache Iceberg table in the catalog. Follows the standard
 * namespace.table naming convention used by Iceberg catalogs.
 *
 * @param namespace the table namespace (e.g., "analytics", "warehouse.sales")
 * @param name the table name within the namespace (e.g., "user_events", "transactions")
 */
opaque type TableId = (String, String)

object TableId:
  def apply(namespace: String, name: String): TableId =
    require(namespace.nonEmpty, "Table namespace cannot be null or empty")
    require(name.nonEmpty, "Table name cannot be null or empty")
    (namespace, name)

  extension (tableId: TableId)
    def namespace: String = tableId._1
    def name: String = tableId._2
    
    /**
     * Returns the fully qualified table name in the format "namespace.name". This is the canonical
     * representation used in catalog operations.
     */
    def fullyQualifiedName: String = s"$namespace.$name"
    
  given Conversion[TableId, String] = _.fullyQualifiedName