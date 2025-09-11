package com.streamfirst.iceberg.hybrid.domain

/** Represents a storage location in a specific region. Contains all information needed to access
  * the storage system in that region.
  */
final case class StorageLocation(
  region: Region,
  endpoint: String,
  bucketName: String,
  pathPrefix: Option[String] = None
):
  def basePath: StoragePath =
    val prefix = pathPrefix.map(_ + "/").getOrElse("")
    StoragePath(s"s3://$bucketName/$prefix")

  def pathFor(relativePath: String): StoragePath =
    basePath / relativePath

object StorageLocation:
  def s3(region: Region, bucket: String, prefix: Option[String] = None): StorageLocation =
    val endpoint = s"s3.${region.id}.amazonaws.com"
    StorageLocation(region, endpoint, bucket, prefix)
