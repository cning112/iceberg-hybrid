package com.streamfirst.iceberg.hybrid.adapters;

import com.streamfirst.iceberg.hybrid.domain.Region;
import com.streamfirst.iceberg.hybrid.domain.StorageLocation;
import com.streamfirst.iceberg.hybrid.domain.TableId;
import com.streamfirst.iceberg.hybrid.ports.RegistryPort;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of RegistryPort for testing and development.
 * Provides simple registry functionality using in-memory maps.
 * Data is lost when the application stops - not suitable for production use.
 */
@Slf4j
public class InMemoryRegistryAdapter implements RegistryPort {
    
    // Storage locations by region and type
    private final Map<Region, Map<String, StorageLocation>> storageLocationsByRegion = new ConcurrentHashMap<>();
    
    // Table locations by table and region
    private final Map<TableId, Map<Region, String>> tableLocations = new ConcurrentHashMap<>();
    
    // Region status
    private final Map<Region, Boolean> regionStatus = new ConcurrentHashMap<>();

    @Override
    public void registerStorageLocation(StorageLocation location) {
        log.debug("Registering storage location: {}", location);
        
        Region region = location.region();
        String type = location.type();
        
        // Check for conflicts
        Map<String, StorageLocation> regionStorageMap = storageLocationsByRegion.get(region);
        if (regionStorageMap != null && regionStorageMap.containsKey(type)) {
            StorageLocation existing = regionStorageMap.get(type);
            if (!existing.equals(location)) {
                throw new IllegalArgumentException(
                    "Storage location conflict for region " + region + " type " + type + 
                    ": existing=" + existing + ", new=" + location);
            }
        }
        
        storageLocationsByRegion.computeIfAbsent(region, k -> new ConcurrentHashMap<>())
                               .put(type, location);
        
        log.info("Registered storage location for region {} type {}: {}", 
                region, type, location.uri());
    }

    @Override
    public void updateStorageLocation(StorageLocation location) {
        log.debug("Updating storage location: {}", location);
        
        Region region = location.region();
        String type = location.type();
        
        Map<String, StorageLocation> regionStorageMap = storageLocationsByRegion.get(region);
        if (regionStorageMap == null || !regionStorageMap.containsKey(type)) {
            throw new IllegalArgumentException(
                "Storage location not found for region " + region + " type " + type);
        }
        
        regionStorageMap.put(type, location);
        
        log.info("Updated storage location for region {} type {}: {}", 
                region, type, location.uri());
    }

    @Override
    public int removeStorageLocations(java.util.function.Predicate<StorageLocation> predicate) {
        log.debug("Removing storage locations matching predicate");
        
        int removedCount = 0;
        java.util.Iterator<java.util.Map.Entry<Region, Map<String, StorageLocation>>> regionIterator = 
            storageLocationsByRegion.entrySet().iterator();
            
        while (regionIterator.hasNext()) {
            java.util.Map.Entry<Region, Map<String, StorageLocation>> regionEntry = regionIterator.next();
            Region region = regionEntry.getKey();
            Map<String, StorageLocation> regionStorageMap = regionEntry.getValue();
            
            java.util.Iterator<java.util.Map.Entry<String, StorageLocation>> typeIterator = 
                regionStorageMap.entrySet().iterator();
                
            while (typeIterator.hasNext()) {
                java.util.Map.Entry<String, StorageLocation> typeEntry = typeIterator.next();
                StorageLocation location = typeEntry.getValue();
                
                if (predicate.test(location)) {
                    typeIterator.remove();
                    removedCount++;
                    log.info("Removed storage location for region {} type {}: {}", 
                            region, typeEntry.getKey(), location.uri());
                }
            }
            
            // Clean up empty maps
            if (regionStorageMap.isEmpty()) {
                regionIterator.remove();
            }
        }
        
        log.debug("Removed {} storage locations matching predicate", removedCount);
        return removedCount;
    }

    @Override
    public Optional<StorageLocation> getStorageLocation(Region region, String type) {
        Map<String, StorageLocation> regionStorageMap = storageLocationsByRegion.get(region);
        if (regionStorageMap == null) {
            log.debug("No storage locations found for region {}", region);
            return Optional.empty();
        }
        
        StorageLocation location = regionStorageMap.get(type);
        if (location != null) {
            log.debug("Found storage location for region {} type {}: {}", 
                     region, type, location.uri());
        } else {
            log.debug("No storage location found for region {} type {}", region, type);
        }
        
        return Optional.ofNullable(location);
    }

    @Override
    public List<StorageLocation> getAllStorageLocations(Region region) {
        Map<String, StorageLocation> regionStorageMap = storageLocationsByRegion.get(region);
        if (regionStorageMap == null) {
            log.debug("No storage locations found for region {}", region);
            return List.of();
        }
        
        List<StorageLocation> locations = new ArrayList<>(regionStorageMap.values());
        log.debug("Found {} storage locations for region {}", locations.size(), region);
        return locations;
    }

    @Override
    public List<StorageLocation> getStorageLocationsByType(String type) {
        List<StorageLocation> locations = storageLocationsByRegion.values().stream()
                .map(regionMap -> regionMap.get(type))
                .filter(Objects::nonNull)
                .toList();
        
        log.debug("Found {} storage locations of type '{}'", locations.size(), type);
        return locations;
    }

    @Override
    public void registerTableLocation(TableId tableId, Region region, String dataPath) {
        log.debug("Registering table location: table={}, region={}, path={}", 
                 tableId, region, dataPath);
        
        tableLocations.computeIfAbsent(tableId, k -> new ConcurrentHashMap<>())
                     .put(region, dataPath);
        
        log.info("Registered table {} in region {} at path {}", tableId, region, dataPath);
    }

    @Override
    public List<Region> getTableRegions(TableId tableId) {
        Map<Region, String> regionMap = tableLocations.get(tableId);
        if (regionMap == null) {
            log.debug("No regions found for table {}", tableId);
            return List.of();
        }
        
        List<Region> regions = new ArrayList<>(regionMap.keySet());
        log.debug("Found {} regions for table {}: {}", regions.size(), tableId, regions);
        return regions;
    }

    @Override
    public Optional<String> getTableDataPath(TableId tableId, Region region) {
        Map<Region, String> regionMap = tableLocations.get(tableId);
        if (regionMap == null) {
            log.debug("No regions found for table {}", tableId);
            return Optional.empty();
        }
        
        String dataPath = regionMap.get(region);
        if (dataPath != null) {
            log.debug("Found data path for table {} in region {}: {}", tableId, region, dataPath);
        } else {
            log.debug("No data path found for table {} in region {}", tableId, region);
        }
        
        return Optional.ofNullable(dataPath);
    }

    @Override
    public List<Region> getAllRegions() {
        Set<Region> allRegions = new HashSet<>(regionStatus.keySet());
        allRegions.addAll(storageLocationsByRegion.keySet());
        
        List<Region> regions = new ArrayList<>(allRegions);
        log.debug("Found {} total regions", regions.size());
        return regions;
    }

    @Override
    public void registerRegion(Region region) {
        if (regionStatus.containsKey(region)) {
            throw new IllegalArgumentException("Region " + region + " already exists");
        }
        
        log.info("Registering region: {}", region);
        regionStatus.put(region, true); // Default to active
        log.info("Registered region {} as active", region);
    }

    @Override
    public void updateRegionStatus(Region region, boolean active) {
        if (!regionStatus.containsKey(region)) {
            log.warn("Region {} not found, registering with status {}", region, active);
            regionStatus.put(region, active);
        } else {
            regionStatus.put(region, active);
            log.info("Updated region {} status to {}", region, active ? "active" : "inactive");
        }
    }

    @Override
    public boolean isRegionActive(Region region) {
        Boolean active = regionStatus.get(region);
        boolean result = active != null && active;
        
        log.debug("Region {} is {}", region, result ? "active" : "inactive");
        return result;
    }

    /**
     * Clears all registry data. Useful for testing.
     */
    public void clear() {
        log.info("Clearing all registry data");
        storageLocationsByRegion.clear();
        tableLocations.clear();
        regionStatus.clear();
    }

    /**
     * Gets registry statistics for monitoring.
     */
    public Map<String, Integer> getRegistryStats() {
        Map<String, Integer> stats = new HashMap<>();
        
        stats.put("regions", regionStatus.size());
        stats.put("active_regions", (int) regionStatus.values().stream().mapToInt(b -> b ? 1 : 0).sum());
        stats.put("storage_locations", storageLocationsByRegion.values().stream()
                                                             .mapToInt(Map::size)
                                                             .sum());
        stats.put("tables", tableLocations.size());
        stats.put("table_locations", tableLocations.values().stream()
                                                   .mapToInt(Map::size)
                                                   .sum());
        
        return stats;
    }

    /**
     * Gets all registered tables for debugging.
     */
    public Set<TableId> getAllTables() {
        return new HashSet<>(tableLocations.keySet());
    }
}