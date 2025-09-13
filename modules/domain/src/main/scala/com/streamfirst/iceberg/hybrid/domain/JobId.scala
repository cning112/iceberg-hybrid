package com.streamfirst.iceberg.hybrid.domain

import java.util.UUID

/** Represents a unique identifier for background jobs */
opaque type JobId = String

object JobId:
  def apply(value: String): JobId = value
  def generate(): JobId = UUID.randomUUID().toString
  
  extension (jobId: JobId)
    def value: String = jobId