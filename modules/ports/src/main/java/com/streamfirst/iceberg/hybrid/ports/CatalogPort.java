package com.streamfirst.iceberg.hybrid.ports;

import com.streamfirst.iceberg.hybrid.domain.*;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Port for global transactional catalog operations.
 * Provides access to the single source of truth for table metadata across all regions.
 */
public interface CatalogPort {
    
    /**
     * Commits new metadata to the global catalog.
     * This is an atomic operation that ensures consistency across regions.
     * 
     * @param request the commit request containing metadata and regional context
     * @return the assigned commit ID for this metadata version
     * @throws IllegalStateException if the commit cannot be applied (e.g., conflicts)
     */
    CommitId commitMetadata(CommitRequest request);
    
    /**
     * Retrieves the latest metadata for a table.
     * 
     * @param tableId the table identifier
     * @return the latest metadata, or empty if table doesn't exist
     */
    Optional<TableMetadata> getLatestMetadata(TableId tableId);
    
    /**
     * Retrieves specific metadata version by commit ID.
     * 
     * @param tableId the table identifier
     * @param commitId the specific commit to retrieve
     * @return the metadata at that commit, or empty if not found
     */
    Optional<TableMetadata> getMetadata(TableId tableId, CommitId commitId);
    
    /**
     * Gets commits for a table matching specific criteria.
     * Used for flexible synchronization queries between regions.
     * 
     * @param tableId the table identifier
     * @param criteria predicate to filter commits
     * @return list of metadata versions matching criteria in chronological order
     */
    List<TableMetadata> getCommits(TableId tableId, Predicate<TableMetadata> criteria);
    
    /**
     * Gets all commits for a table since a specific commit ID.
     * Used for incremental synchronization between regions.
     * 
     * @param tableId the table identifier
     * @param sinceCommitId the starting commit (exclusive)
     * @return list of metadata versions in chronological order
     */
    default List<TableMetadata> getCommitsSince(TableId tableId, CommitId sinceCommitId) {
        return getCommits(tableId, metadata -> metadata.getCommitId().value().compareTo(sinceCommitId.value()) > 0);
    }
    
    /**
     * Lists tables matching specific criteria.
     * Allows flexible filtering of tables across namespaces.
     * 
     * @param criteria predicate to filter tables
     * @return list of table identifiers matching criteria
     */
    List<TableId> listTables(Predicate<TableId> criteria);
    
    /**
     * Lists all tables in a namespace.
     * 
     * @param namespace the namespace to search
     * @return list of table identifiers in the namespace
     */
    default List<TableId> listTables(String namespace) {
        return listTables(tableId -> tableId.namespace().equals(namespace));
    }
    
    /**
     * Checks if a table exists in the catalog.
     * 
     * @param tableId the table identifier
     * @return true if the table exists, false otherwise
     */
    boolean tableExists(TableId tableId);
    
    /**
     * Creates a new table in the catalog.
     * 
     * @param tableId the table identifier
     * @param schema the initial table schema (JSON format)
     * @param sourceRegion the region where the table is being created
     * @throws IllegalArgumentException if table already exists
     */
    void createTable(TableId tableId, String schema, Region sourceRegion);
    
    /**
     * Drops a table from the catalog.
     * This removes all metadata history for the table.
     * 
     * @param tableId the table identifier
     * @throws IllegalArgumentException if table doesn't exist
     */
    void dropTable(TableId tableId);
}