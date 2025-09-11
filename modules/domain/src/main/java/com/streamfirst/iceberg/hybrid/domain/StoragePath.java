package com.streamfirst.iceberg.hybrid.domain;

/**
 * Value object representing a storage path with validation and operations.
 * Ensures path consistency across different storage backends and operating systems.
 */
public record StoragePath(String path) {
    
    public StoragePath {
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException("Storage path cannot be null or empty");
        }
        if (path.contains("..")) {
            throw new IllegalArgumentException("Storage path cannot contain '..' for security");
        }
        // Normalize path separators to forward slash for consistency
        path = path.replace('\\', '/');
        // Remove trailing slash except for root
        if (path.length() > 1 && path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
    }
    
    /**
     * Creates a storage path from string.
     */
    public static StoragePath of(String path) {
        return new StoragePath(path);
    }
    
    /**
     * Creates a storage path by joining components.
     */
    public static StoragePath of(String... components) {
        if (components.length == 0) {
            throw new IllegalArgumentException("At least one path component required");
        }
        return new StoragePath(String.join("/", components));
    }
    
    /**
     * Resolves a child path relative to this path.
     */
    public StoragePath resolve(String childPath) {
        if (childPath.startsWith("/")) {
            throw new IllegalArgumentException("Child path must be relative: " + childPath);
        }
        return new StoragePath(this.path + "/" + childPath);
    }
    
    /**
     * Resolves a child path relative to this path.
     */
    public StoragePath resolve(StoragePath childPath) {
        return resolve(childPath.path);
    }
    
    /**
     * Gets the parent directory path.
     */
    public StoragePath getParent() {
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash <= 0) {
            return StoragePath.of("/");
        }
        return new StoragePath(path.substring(0, lastSlash));
    }
    
    /**
     * Gets the file/directory name (last component).
     */
    public String getFileName() {
        int lastSlash = path.lastIndexOf('/');
        return lastSlash == -1 ? path : path.substring(lastSlash + 1);
    }
    
    /**
     * Gets the file extension without the dot.
     */
    public String getExtension() {
        String fileName = getFileName();
        int lastDot = fileName.lastIndexOf('.');
        return lastDot == -1 ? "" : fileName.substring(lastDot + 1);
    }
    
    /**
     * Checks if this path starts with the given prefix.
     */
    public boolean startsWith(StoragePath prefix) {
        return this.path.startsWith(prefix.path);
    }
    
    /**
     * Checks if this path starts with the given prefix string.
     */
    public boolean startsWith(String prefix) {
        return this.path.startsWith(prefix);
    }
    
    /**
     * Checks if this path ends with the given suffix.
     */
    public boolean endsWith(String suffix) {
        return this.path.endsWith(suffix);
    }
    
    /**
     * Returns the raw path string.
     */
    @Override
    public String toString() {
        return path;
    }
}