package com.streamfirst.iceberg.hybrid.ports;

import com.streamfirst.iceberg.hybrid.domain.Region;
import com.streamfirst.iceberg.hybrid.domain.StorageLocation;

import java.io.InputStream;
import java.util.List;

/**
 * Port for file operations across regional storage systems.
 * Abstracts different storage backends (S3, MinIO, HDFS, etc.) behind a uniform interface.
 */
public interface StoragePort {
    
    /**
     * Writes data to a file at the specified location.
     * 
     * @param location the storage location (region + URI + type)
     * @param path the file path within the storage location
     * @param data the data to write
     * @throws RuntimeException if write operation fails
     */
    void writeFile(StorageLocation location, String path, byte[] data);
    
    /**
     * Writes streaming data to a file at the specified location.
     * 
     * @param location the storage location
     * @param path the file path within the storage location
     * @param data the input stream containing data to write
     * @throws RuntimeException if write operation fails
     */
    void writeFile(StorageLocation location, String path, InputStream data);
    
    /**
     * Reads entire file content into memory.
     * Use with caution for large files.
     * 
     * @param location the storage location
     * @param path the file path to read
     * @return the file content as byte array
     * @throws RuntimeException if file doesn't exist or read fails
     */
    byte[] readFile(StorageLocation location, String path);
    
    /**
     * Opens a stream for reading file content.
     * Preferred method for large files.
     * 
     * @param location the storage location
     * @param path the file path to read
     * @return input stream for reading file content
     * @throws RuntimeException if file doesn't exist or read fails
     */
    InputStream readFileStream(StorageLocation location, String path);
    
    /**
     * Checks if a file exists at the specified location.
     * 
     * @param location the storage location
     * @param path the file path to check
     * @return true if file exists, false otherwise
     */
    boolean fileExists(StorageLocation location, String path);
    
    /**
     * Deletes a file from storage.
     * 
     * @param location the storage location
     * @param path the file path to delete
     * @throws RuntimeException if deletion fails
     */
    void deleteFile(StorageLocation location, String path);
    
    /**
     * Copies a file between storage locations.
     * Supports cross-region copying for data replication.
     * 
     * @param source the source storage location
     * @param sourcePath the source file path
     * @param target the target storage location
     * @param targetPath the target file path
     * @throws RuntimeException if copy operation fails
     */
    void copyFile(StorageLocation source, String sourcePath, 
                  StorageLocation target, String targetPath);
    
    /**
     * Lists files with a given prefix.
     * 
     * @param location the storage location
     * @param prefix the path prefix to filter files
     * @return list of file paths matching the prefix
     */
    List<String> listFiles(StorageLocation location, String prefix);
    
    /**
     * Gets the size of a file in bytes.
     * 
     * @param location the storage location
     * @param path the file path
     * @return file size in bytes
     * @throws RuntimeException if file doesn't exist
     */
    long getFileSize(StorageLocation location, String path);
    
    /**
     * Gets the primary storage location for a region.
     * 
     * @param region the region
     * @return the storage location configured for that region
     * @throws IllegalArgumentException if region has no configured storage
     */
    StorageLocation getStorageLocation(Region region);
}