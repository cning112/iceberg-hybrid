package com.streamfirst.iceberg.hybrid.domain

import java.util.UUID

/** Strong type for event identifiers. Provides type safety and prevents mixing up event IDs with
  * other string values.
  */
opaque type EventId = String

object EventId:
  def apply(value: String): EventId =
    require(value.trim.nonEmpty, "Event ID cannot be null or empty")
    value

  def generate(): EventId =
    s"evt-${UUID.randomUUID()}"

  def generate(prefix: String): EventId =
    s"$prefix-${UUID.randomUUID()}"

  extension (eventId: EventId)
    def value: String = eventId
    def asString: String = eventId

  given Conversion[EventId, String] = _.value
