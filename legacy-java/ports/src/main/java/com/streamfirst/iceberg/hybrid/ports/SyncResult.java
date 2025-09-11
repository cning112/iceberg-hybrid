package com.streamfirst.iceberg.hybrid.ports;

import java.util.Optional;
import lombok.Value;

/**
 * Result type for synchronization operations. Provides success/failure status with optional error
 * details.
 */
@Value
public class SyncResult {
  boolean success;
  String message;
  Optional<String> errorCode;

  /** Creates a successful result with default message. */
  public static SyncResult success() {
    return new SyncResult(true, "Success", Optional.empty());
  }

  /** Creates a successful result with custom message. */
  public static SyncResult success(String message) {
    return new SyncResult(true, message, Optional.empty());
  }

  /** Creates a failure result with error message. */
  public static SyncResult failure(String message) {
    return new SyncResult(false, message, Optional.empty());
  }

  /** Creates a failure result with error message and code. */
  public static SyncResult failure(String message, String errorCode) {
    return new SyncResult(false, message, Optional.of(errorCode));
  }
}
