package com.streamfirst.iceberg.hybrid.application;

import com.streamfirst.iceberg.hybrid.domain.*;
import com.streamfirst.iceberg.hybrid.ports.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

/**
 * Routes read requests to the optimal region for low-latency access.
 * Implements intelligent routing based on data availability and region proximity.
 * Provides fallback mechanisms when data is not available locally.
 */
@Slf4j
@RequiredArgsConstructor
public class ReadRouter {
    
    private final CatalogPort catalogPort;
    private final StoragePort storagePort;
    private final RegistryPort registryPort;

    /**
     * Gets the latest table metadata, preferring local region if available.
     * 
     * @param tableId the table to read
     * @param preferredRegion the region where the read is being requested from
     * @return table metadata if found, empty if table doesn't exist
     */
    public Optional<TableMetadata> getLatestMetadata(TableId tableId, Region preferredRegion) {
        log.debug("Getting latest metadata for table {} preferring region {}", 
                 tableId, preferredRegion);
        
        return catalogPort.getLatestMetadata(tableId);
    }

    /**
     * Gets table metadata for a specific commit, with regional optimization.
     * 
     * @param tableId the table to read
     * @param commitId the specific commit version
     * @param preferredRegion the region where the read is being requested from
     * @return table metadata for the specified commit
     */
    public Optional<TableMetadata> getMetadata(TableId tableId, CommitId commitId, Region preferredRegion) {
        log.debug("Getting metadata for table {} commit {} preferring region {}", 
                 tableId, commitId, preferredRegion);
        
        return catalogPort.getMetadata(tableId, commitId);
    }

    /**
     * Determines the best region to read data from based on availability and proximity.
     * 
     * @param tableId the table to read
     * @param preferredRegion the region where the read request originates
     * @return the optimal region for reading, or empty if table has no data
     */
    public Optional<Region> getBestReadRegion(TableId tableId, Region preferredRegion) {
        List<Region> availableRegions = registryPort.getTableRegions(tableId);
        
        if (availableRegions.isEmpty()) {
            log.warn("No regions found with data for table {}", tableId);
            return Optional.empty();
        }
        
        // First preference: data available in preferred region
        if (availableRegions.contains(preferredRegion) && 
            isRegionDataComplete(tableId, preferredRegion)) {
            log.debug("Using preferred region {} for table {}", preferredRegion, tableId);
            return Optional.of(preferredRegion);
        }
        
        // Second preference: find closest active region with complete data
        for (Region region : availableRegions) {
            if (registryPort.isRegionActive(region) && 
                isRegionDataComplete(tableId, region)) {
                log.debug("Using fallback region {} for table {}", region, tableId);
                return Optional.of(region);
            }
        }
        
        // Last resort: any region with data
        Region fallbackRegion = availableRegions.get(0);
        log.warn("Using last resort region {} for table {} (data may be incomplete)", 
                 fallbackRegion, tableId);
        return Optional.of(fallbackRegion);
    }

    /**
     * Gets the storage location for reading data from a specific region.
     * 
     * @param tableId the table to read
     * @param region the region to read from
     * @return storage location if the table exists in that region
     */
    public Optional<StorageLocation> getStorageLocation(TableId tableId, Region region) {
        try {
            // Check if table has data in this region
            Optional<String> dataPath = registryPort.getTableDataPath(tableId, region);
            if (dataPath.isEmpty()) {
                log.debug("No data path found for table {} in region {}", tableId, region);
                return Optional.empty();
            }
            
            // Get the storage location for this region
            StorageLocation storage = storagePort.getStorageLocation(region);
            log.debug("Found storage location for table {} in region {}: {}", 
                     tableId, region, storage);
            return Optional.of(storage);
            
        } catch (Exception e) {
            log.error("Failed to get storage location for table {} in region {}", 
                     tableId, region, e);
            return Optional.empty();
        }
    }

    /**
     * Lists all tables in a namespace, combining results from all regions.
     * 
     * @param namespace the namespace to list tables from
     * @return list of table identifiers in the namespace
     */
    public List<TableId> listTables(String namespace) {
        log.debug("Listing tables in namespace {}", namespace);
        
        try {
            return catalogPort.listTables(namespace);
        } catch (Exception e) {
            log.error("Failed to list tables in namespace {}", namespace, e);
            return List.of();
        }
    }

    /**
     * Checks if a table exists and is accessible.
     * 
     * @param tableId the table to check
     * @return true if the table exists and is accessible
     */
    public boolean tableExists(TableId tableId) {
        try {
            return catalogPort.tableExists(tableId);
        } catch (Exception e) {
            log.error("Failed to check existence of table {}", tableId, e);
            return false;
        }
    }

    /**
     * Gets the data file paths for a table in a specific region.
     * Used by query engines to locate the actual data files.
     * 
     * @param tableId the table to get files for
     * @param region the region to read files from
     * @param commitId optional specific commit (uses latest if not provided)
     * @return list of data file paths, empty if table/region not found
     */
    public List<String> getDataFiles(TableId tableId, Region region, Optional<CommitId> commitId) {
        try {
            // Get metadata for the specified commit or latest
            Optional<TableMetadata> metadataOpt = commitId.isPresent() ?
                catalogPort.getMetadata(tableId, commitId.get()) :
                catalogPort.getLatestMetadata(tableId);
                
            if (metadataOpt.isEmpty()) {
                log.warn("No metadata found for table {} commit {}", tableId, commitId);
                return List.of();
            }
            
            TableMetadata metadata = metadataOpt.get();
            
            // Check if data exists in the requested region
            Optional<String> dataPath = registryPort.getTableDataPath(tableId, region);
            if (dataPath.isEmpty()) {
                log.warn("No data path found for table {} in region {}", tableId, region);
                return List.of();
            }
            
            // Transform file paths to be region-specific
            String basePath = dataPath.get();
            return metadata.getDataFiles().stream()
                .map(file -> basePath + "/" + extractFileName(file))
                .toList();
                
        } catch (Exception e) {
            log.error("Failed to get data files for table {} in region {}", tableId, region, e);
            return List.of();
        }
    }

    /**
     * Checks if a region has complete data for a table.
     * This is a simplified check - in practice, would verify file existence and completeness.
     */
    private boolean isRegionDataComplete(TableId tableId, Region region) {
        try {
            Optional<String> dataPath = registryPort.getTableDataPath(tableId, region);
            return dataPath.isPresent() && registryPort.isRegionActive(region);
        } catch (Exception e) {
            log.error("Failed to check data completeness for table {} in region {}", 
                     tableId, region, e);
            return false;
        }
    }

    /**
     * Extracts filename from a full file path.
     */
    private String extractFileName(String filePath) {
        int lastSlash = filePath.lastIndexOf('/');
        return lastSlash >= 0 ? filePath.substring(lastSlash + 1) : filePath;
    }
}