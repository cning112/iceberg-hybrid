package com.streamfirst.iceberg.hybrid.domain

/** Strong type for storage paths. Represents file paths in the distributed storage system.
  */
opaque type StoragePath = String

object StoragePath:
  def apply(path: String): StoragePath =
    require(path.trim.nonEmpty, "Storage path cannot be null or empty")
    path.trim

  extension (path: StoragePath)
    def value: String = path
    def asString: String = path
    def fileName: String =
      val lastSlash = path.lastIndexOf('/')
      if lastSlash >= 0 then path.substring(lastSlash + 1) else path
    def parent: Option[StoragePath] =
      val lastSlash = path.lastIndexOf('/')
      if lastSlash > 0 then Some(StoragePath(path.substring(0, lastSlash)))
      else None
    def /(child: String): StoragePath =
      StoragePath(s"$path/$child")

  given Conversion[StoragePath, String] = _.value
