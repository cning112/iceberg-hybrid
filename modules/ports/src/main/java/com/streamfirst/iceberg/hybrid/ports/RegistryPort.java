package com.streamfirst.iceberg.hybrid.ports;

import com.streamfirst.iceberg.hybrid.domain.Region;
import com.streamfirst.iceberg.hybrid.domain.StorageLocation;
import com.streamfirst.iceberg.hybrid.domain.TableId;

import java.util.List;
import java.util.Optional;

/**
 * Port for global storage and region registry.
 * Maintains the mapping of storage locations, table locations, and region configurations.
 * Acts as the service discovery mechanism for the geo-distributed system.
 */
public interface RegistryPort {
    
    /**
     * Registers a new storage location in the global registry.
     * 
     * @param location the storage location to register
     * @throws IllegalArgumentException if location conflicts with existing registration
     */
    void registerStorageLocation(StorageLocation location);
    
    /**
     * Updates an existing storage location configuration.
     * 
     * @param location the updated storage location
     * @throws IllegalArgumentException if location doesn't exist
     */
    void updateStorageLocation(StorageLocation location);
    
    /**
     * Removes a storage location from the registry.
     * 
     * @param region the region of the storage location
     * @param type the type of storage (e.g., "primary", "backup")
     */
    void removeStorageLocation(Region region, String type);
    
    /**
     * Gets a specific storage location by region and type.
     * 
     * @param region the region
     * @param type the storage type
     * @return the storage location, or empty if not found
     */
    Optional<StorageLocation> getStorageLocation(Region region, String type);
    
    /**
     * Gets all storage locations for a region.
     * 
     * @param region the region
     * @return list of all storage locations in that region
     */
    List<StorageLocation> getAllStorageLocations(Region region);
    
    /**
     * Gets all storage locations of a specific type across all regions.
     * 
     * @param type the storage type
     * @return list of storage locations of that type
     */
    List<StorageLocation> getStorageLocationsByType(String type);
    
    /**
     * Registers the data location for a table in a specific region.
     * 
     * @param tableId the table identifier
     * @param region the region where data is stored
     * @param dataPath the path to the table data within the region's storage
     */
    void registerTableLocation(TableId tableId, Region region, String dataPath);
    
    /**
     * Gets all regions where a table has data.
     * 
     * @param tableId the table identifier
     * @return list of regions containing data for this table
     */
    List<Region> getTableRegions(TableId tableId);
    
    /**
     * Gets the data path for a table in a specific region.
     * 
     * @param tableId the table identifier
     * @param region the region
     * @return the data path, or empty if table has no data in that region
     */
    Optional<String> getTableDataPath(TableId tableId, Region region);
    
    /**
     * Gets all registered regions in the system.
     * 
     * @return list of all regions
     */
    List<Region> getAllRegions();
    
    /**
     * Registers a new region in the system.
     * 
     * @param region the region to register
     * @throws IllegalArgumentException if region already exists
     */
    void registerRegion(Region region);
    
    /**
     * Updates the active status of a region.
     * Inactive regions don't participate in commit consensus.
     * 
     * @param region the region
     * @param active true to activate, false to deactivate
     */
    void updateRegionStatus(Region region, boolean active);
    
    /**
     * Checks if a region is currently active.
     * 
     * @param region the region to check
     * @return true if region is active, false otherwise
     */
    boolean isRegionActive(Region region);
}