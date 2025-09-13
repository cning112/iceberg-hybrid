package com.streamfirst.iceberg.hybrid.domain

import java.util.UUID

/** Represents a unique identifier for write coordination jobs */
opaque type WriteJobId = String

object WriteJobId:
  def apply(value: String): WriteJobId = value
  def generate(): WriteJobId = UUID.randomUUID().toString
  
  extension (writeJobId: WriteJobId)
    def value: String = writeJobId