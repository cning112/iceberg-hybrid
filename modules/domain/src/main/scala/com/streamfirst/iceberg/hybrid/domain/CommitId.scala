package com.streamfirst.iceberg.hybrid.domain

import java.util.UUID

/** Strong type for commit identifiers in Apache Iceberg. Represents a specific version of table
  * metadata and schema changes.
  */
opaque type CommitId = String

object CommitId:
  def apply(value: String): CommitId =
    require(value.trim.nonEmpty, "Commit ID cannot be null or empty")
    value

  def generate(): CommitId = s"commit-${UUID.randomUUID()}"

  extension (commitId: CommitId)
    def value: String = commitId
    def asString: String = commitId

  given Conversion[CommitId, String] = _.value
