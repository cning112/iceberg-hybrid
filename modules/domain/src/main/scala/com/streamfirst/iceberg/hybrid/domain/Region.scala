package com.streamfirst.iceberg.hybrid.domain

/** Represents a geographic region in the distributed Iceberg system. Each region has its own
  * storage, compute, and replication capabilities. Regions participate in distributed consensus for
  * write operations.
  *
  * @param id
  *   unique identifier for the region (e.g., "us-east-1", "eu-west-1")
  * @param displayName
  *   human-readable name for the region (e.g., "US East", "Europe West")
  */
opaque type Region = (String, String)

object Region:
  def apply(id: String, displayName: String): Region =
    require(id.nonEmpty, "Region id cannot be null or empty")
    require(displayName.nonEmpty, "Region displayName cannot be null or empty")
    (id, displayName)

  extension (region: Region)
    def id: String = region._1
    def displayName: String = region._2

  // Common regions for convenience
  val UsEast1: Region = Region("us-east-1", "US East (N. Virginia)")
  val EuWest1: Region = Region("eu-west-1", "Europe (Ireland)")
  val ApNortheast1: Region = Region("ap-northeast-1", "Asia Pacific (Tokyo)")
