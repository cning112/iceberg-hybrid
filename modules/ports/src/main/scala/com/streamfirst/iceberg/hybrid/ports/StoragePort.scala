package com.streamfirst.iceberg.hybrid.ports

import com.streamfirst.iceberg.hybrid.domain.*
import com.streamfirst.iceberg.hybrid.domain.DomainError.StorageError
import zio.*
import zio.stream.*
import java.io.InputStream

/**
 * Port for file operations across regional storage systems. Abstracts different storage backends
 * (S3, MinIO, HDFS, etc.) behind a uniform interface using ZIO effects.
 */
trait StoragePort:
  /**
   * Writes data to a file at the specified location.
   */
  def writeFile(location: StorageLocation, path: StoragePath, data: Array[Byte]): IO[StorageError, Unit]

  /**
   * Writes streaming data to a file at the specified location.
   */
  def writeFileStream(location: StorageLocation, path: StoragePath, data: ZStream[Any, Throwable, Byte]): IO[StorageError, Unit]

  /**
   * Reads entire file content into memory. Use with caution for large files.
   */
  def readFile(location: StorageLocation, path: StoragePath): IO[StorageError, Array[Byte]]

  /**
   * Reads file content as a stream. Preferred for large files.
   */
  def readFileStream(location: StorageLocation, path: StoragePath): ZStream[Any, StorageError, Byte]

  /**
   * Checks if a file exists at the specified path.
   */
  def fileExists(location: StorageLocation, path: StoragePath): IO[StorageError, Boolean]

  /**
   * Lists files matching the specified pattern in a directory.
   */
  def listFiles(
    location: StorageLocation, 
    directory: StoragePath, 
    predicate: StoragePath => Boolean
  ): IO[StorageError, List[StoragePath]]

  /**
   * Copies a file from source to target location. Handles cross-region copying.
   */
  def copyFile(
    sourceLocation: StorageLocation,
    sourcePath: StoragePath,
    targetLocation: StorageLocation,
    targetPath: StoragePath
  ): IO[StorageError, Unit]

  /**
   * Deletes a file at the specified location.
   */
  def deleteFile(location: StorageLocation, path: StoragePath): IO[StorageError, Unit]

  /**
   * Gets the storage location configuration for a specific region.
   */
  def getStorageLocation(region: Region): IO[StorageError, StorageLocation]

  /**
   * Gets file metadata (size, last modified, etc.) without reading content.
   */
  def getFileMetadata(location: StorageLocation, path: StoragePath): IO[StorageError, FileMetadata]

case class FileMetadata(
  path: StoragePath,
  size: Long,
  lastModified: java.time.Instant,
  contentType: Option[String] = None
)

object StoragePort:
  /**
   * ZIO service accessors for dependency injection
   */
  def writeFile(location: StorageLocation, path: StoragePath, data: Array[Byte]): ZIO[StoragePort, StorageError, Unit] =
    ZIO.serviceWithZIO[StoragePort](_.writeFile(location, path, data))

  def readFile(location: StorageLocation, path: StoragePath): ZIO[StoragePort, StorageError, Array[Byte]] =
    ZIO.serviceWithZIO[StoragePort](_.readFile(location, path))

  def fileExists(location: StorageLocation, path: StoragePath): ZIO[StoragePort, StorageError, Boolean] =
    ZIO.serviceWithZIO[StoragePort](_.fileExists(location, path))

  def copyFile(
    sourceLocation: StorageLocation,
    sourcePath: StoragePath,
    targetLocation: StorageLocation,
    targetPath: StoragePath
  ): ZIO[StoragePort, StorageError, Unit] =
    ZIO.serviceWithZIO[StoragePort](_.copyFile(sourceLocation, sourcePath, targetLocation, targetPath))

  def getStorageLocation(region: Region): ZIO[StoragePort, StorageError, StorageLocation] =
    ZIO.serviceWithZIO[StoragePort](_.getStorageLocation(region))