package com.streamfirst.iceberg.hybrid.domain

/**
 * Base trait for all domain errors in the system. Provides type-safe error handling
 * with ZIO's error channels instead of Java's Result<T> wrapper.
 */
sealed trait DomainError:
  def message: String
  def code: Option[String] = None

object DomainError:
  /**
   * Synchronization-related errors
   */
  sealed trait SyncError extends DomainError

  case class SyncEventNotFound(eventId: EventId) extends SyncError:
    val message = s"Sync event not found: $eventId"
    override val code = Some("SYNC_EVENT_NOT_FOUND")

  case class InvalidSyncTransition(from: SyncEvent.Status, to: SyncEvent.Status) extends SyncError:
    val message = s"Invalid sync status transition from $from to $to"
    override val code = Some("INVALID_SYNC_TRANSITION")

  case class RegionNotAvailable(region: Region) extends SyncError:
    val message = s"Region not available: ${region.id}"
    override val code = Some("REGION_NOT_AVAILABLE")

  /**
   * Storage-related errors
   */
  sealed trait StorageError extends DomainError

  case class StorageLocationNotFound(region: Region) extends StorageError:
    val message = s"Storage location not found for region: ${region.id}"
    override val code = Some("STORAGE_LOCATION_NOT_FOUND")

  case class FileNotFound(path: StoragePath) extends StorageError:
    val message = s"File not found: ${path.value}"
    override val code = Some("FILE_NOT_FOUND")

  case class FileCopyFailed(source: StoragePath, target: StoragePath, reason: String) extends StorageError:
    val message = s"Failed to copy file from $source to $target: $reason"
    override val code = Some("FILE_COPY_FAILED")

  /**
   * Catalog-related errors
   */
  sealed trait CatalogError extends DomainError

  case class TableNotFound(tableId: TableId) extends CatalogError:
    val message = s"Table not found: ${tableId.fullyQualifiedName}"
    override val code = Some("TABLE_NOT_FOUND")

  case class CommitNotFound(tableId: TableId, commitId: CommitId) extends CatalogError:
    val message = s"Commit $commitId not found for table ${tableId.fullyQualifiedName}"
    override val code = Some("COMMIT_NOT_FOUND")

  case class CatalogUnavailable(reason: String) extends CatalogError:
    val message = s"Catalog unavailable: $reason"
    override val code = Some("CATALOG_UNAVAILABLE")

  /**
   * Generic domain errors
   */
  case class ValidationError(field: String, reason: String) extends DomainError:
    val message = s"Validation failed for field '$field': $reason"
    override val code = Some("VALIDATION_ERROR")

  case class ConfigurationError(setting: String, reason: String) extends DomainError:
    val message = s"Configuration error for '$setting': $reason"
    override val code = Some("CONFIGURATION_ERROR")