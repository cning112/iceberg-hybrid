package com.streamfirst.iceberg.hybrid.domain;

import java.util.UUID;
import lombok.NonNull;

/**
 * Strong type for event identifiers. Provides type safety and prevents mixing up event IDs with
 * other string values.
 */
public record EventId(@NonNull String value) {

  public EventId {
    if (value.trim().isEmpty()) {
      throw new IllegalArgumentException("Event ID cannot be null or empty");
    }
  }

  /** Creates an EventId from a string value. */
  public static EventId of(String value) {
    return new EventId(value);
  }

  /** Generates a new unique EventId using UUID. */
  public static EventId generate() {
    return new EventId("evt-" + UUID.randomUUID().toString());
  }

  /** Creates an EventId with a custom prefix and UUID. */
  public static EventId generate(String prefix) {
    return new EventId(prefix + "-" + UUID.randomUUID().toString());
  }

  @Override
  public @NonNull String toString() {
    return value;
  }
}
