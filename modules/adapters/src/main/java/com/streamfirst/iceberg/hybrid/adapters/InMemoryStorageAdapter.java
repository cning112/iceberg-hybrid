package com.streamfirst.iceberg.hybrid.adapters;

import com.streamfirst.iceberg.hybrid.domain.Region;
import com.streamfirst.iceberg.hybrid.domain.StorageLocation;
import com.streamfirst.iceberg.hybrid.domain.StoragePath;
import com.streamfirst.iceberg.hybrid.ports.StoragePort;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * In-memory implementation of StoragePort for testing and development.
 * Simulates file storage using in-memory byte arrays and maps.
 * Data is lost when the application stops - not suitable for production use.
 */
@Slf4j
public class InMemoryStorageAdapter implements StoragePort {
    
    // Map from storage location URI to file path to file content
    private final Map<String, Map<String, byte[]>> storageContents = new ConcurrentHashMap<>();
    
    // Map from region to its primary storage location
    private final Map<Region, StorageLocation> regionStorageMap = new ConcurrentHashMap<>();

    @Override
    public void writeFile(StorageLocation location, StoragePath path, byte[] data) {
        log.debug("Writing file {} to storage {} ({} bytes)", 
                 path, location.uri(), data.length);
        
        storageContents.computeIfAbsent(location.uri(), k -> new ConcurrentHashMap<>())
                      .put(path.toString(), Arrays.copyOf(data, data.length));
        
        log.debug("Successfully wrote file {} to storage {}", path, location.uri());
    }

    @Override
    public void writeFile(StorageLocation location, StoragePath path, InputStream data) {
        try {
            byte[] bytes = data.readAllBytes();
            writeFile(location, path, bytes);
        } catch (IOException e) {
            log.error("Failed to write file {} to storage {}", path, location.uri(), e);
            throw new RuntimeException("Failed to write file: " + path, e);
        }
    }

    @Override
    public byte[] readFile(StorageLocation location, StoragePath path) {
        log.debug("Reading file {} from storage {}", path, location.uri());
        
        Map<String, byte[]> storage = storageContents.get(location.uri());
        if (storage == null) {
            throw new RuntimeException("Storage location not found: " + location.uri());
        }
        
        byte[] content = storage.get(path.toString());
        if (content == null) {
            throw new RuntimeException("File not found: " + path + " in storage " + location.uri());
        }
        
        log.debug("Successfully read file {} from storage {} ({} bytes)", 
                 path, location.uri(), content.length);
        return Arrays.copyOf(content, content.length);
    }

    @Override
    public InputStream readFileStream(StorageLocation location, StoragePath path) {
        byte[] content = readFile(location, path);
        return new ByteArrayInputStream(content);
    }

    @Override
    public boolean fileExists(StorageLocation location, StoragePath path) {
        Map<String, byte[]> storage = storageContents.get(location.uri());
        boolean exists = storage != null && storage.containsKey(path.toString());
        
        log.debug("File {} exists in storage {}: {}", path, location.uri(), exists);
        return exists;
    }

    @Override
    public void deleteFile(StorageLocation location, StoragePath path) {
        log.debug("Deleting file {} from storage {}", path, location.uri());
        
        Map<String, byte[]> storage = storageContents.get(location.uri());
        if (storage == null) {
            throw new RuntimeException("Storage location not found: " + location.uri());
        }
        
        byte[] removed = storage.remove(path.toString());
        if (removed == null) {
            throw new RuntimeException("File not found: " + path + " in storage " + location.uri());
        }
        
        log.debug("Successfully deleted file {} from storage {}", path, location.uri());
    }

    @Override
    public void copyFile(StorageLocation source, StoragePath sourcePath, 
                        StorageLocation target, StoragePath targetPath) {
        log.debug("Copying file from {}:{} to {}:{}", 
                 source.uri(), sourcePath, target.uri(), targetPath);
        
        // Read from source
        byte[] content = readFile(source, sourcePath);
        
        // Write to target
        writeFile(target, targetPath, content);
        
        log.debug("Successfully copied file from {}:{} to {}:{}", 
                 source.uri(), sourcePath, target.uri(), targetPath);
    }

    @Override
    public List<StoragePath> listFiles(StorageLocation location, Predicate<StoragePath> predicate) {
        log.debug("Listing files matching predicate in storage {}", location.uri());

        Map<String, byte[]> storage = storageContents.get(location.uri());
        if (storage == null) {
            log.debug("Storage location not found: {}", location.uri());
            return List.of();
        }

        List<StoragePath> matchingFiles = storage.keySet().stream()
                .map(StoragePath::of)
                .filter(predicate)
                .sorted((a, b) -> a.toString().compareTo(b.toString()))
                .toList();

        log.debug("Found {} files matching predicate in storage {}",
                 matchingFiles.size(), location.uri());
        return matchingFiles;
    }

    @Override
    public long getFileSize(StorageLocation location, StoragePath path) {
        log.debug("Getting size of file {} in storage {}", path, location.uri());
        
        Map<String, byte[]> storage = storageContents.get(location.uri());
        if (storage == null) {
            throw new RuntimeException("Storage location not found: " + location.uri());
        }
        
        byte[] content = storage.get(path.toString());
        if (content == null) {
            throw new RuntimeException("File not found: " + path + " in storage " + location.uri());
        }
        
        long size = content.length;
        log.debug("File {} in storage {} has size {} bytes", path, location.uri(), size);
        return size;
    }

    @Override
    public StorageLocation getStorageLocation(Region region) {
        StorageLocation location = regionStorageMap.get(region);
        if (location == null) {
            throw new IllegalArgumentException("No storage configured for region: " + region);
        }
        
        log.debug("Retrieved storage location for region {}: {}", region, location);
        return location;
    }

    /**
     * Registers a storage location for a region. Used for testing setup.
     */
    public void registerStorageLocation(Region region, StorageLocation location) {
        log.info("Registering storage location for region {}: {}", region, location);
        
        regionStorageMap.put(region, location);
        
        // Initialize storage if it doesn't exist
        storageContents.computeIfAbsent(location.uri(), k -> new ConcurrentHashMap<>());
    }

    /**
     * Clears all storage data. Useful for testing.
     */
    public void clear() {
        log.info("Clearing all storage data");
        storageContents.clear();
        regionStorageMap.clear();
    }

    /**
     * Gets the total number of files across all storage locations.
     */
    public int getTotalFileCount() {
        return storageContents.values().stream()
                .mapToInt(Map::size)
                .sum();
    }

    /**
     * Gets the total size of all files across all storage locations.
     */
    public long getTotalStorageSize() {
        return storageContents.values().stream()
                .flatMap(storage -> storage.values().stream())
                .mapToLong(content -> content.length)
                .sum();
    }

    /**
     * Gets all files in a storage location for debugging.
     */
    public Map<String, Integer> getStorageInfo(String storageUri) {
        Map<String, byte[]> storage = storageContents.get(storageUri);
        if (storage == null) {
            return Map.of();
        }
        
        Map<String, Integer> info = new HashMap<>();
        storage.forEach((path, content) -> info.put(path, content.length));
        return info;
    }
}