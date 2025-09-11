package com.streamfirst.iceberg.hybrid.application;

import com.streamfirst.iceberg.hybrid.domain.*;
import com.streamfirst.iceberg.hybrid.ports.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Coordinates write operations across the geo-distributed system.
 * Implements the distributed commit protocol ensuring consistency across regions.
 * Handles the complete write workflow from commit request to global replication.
 */
@Slf4j
@RequiredArgsConstructor
public class WriteCoordinator {
    
    private final CatalogPort catalogPort;
    private final CommitGatePort commitGatePort;
    private final SyncPort syncPort;
    private final RegistryPort registryPort;

    /**
     * Executes a write operation with geo-distributed consistency.
     * 
     * @param request the commit request containing write details
     * @return future that completes when write is committed globally
     */
    public CompletableFuture<CommitId> executeWrite(CommitRequest request) {
        log.info("Starting write coordination for {}", request);
        
        return requestCommitApproval(request)
            .thenCompose(this::validateApprovalResult)
            .thenCompose(approvalResult -> commitToGlobalCatalog(request))
            .thenCompose(commitId -> triggerReplication(request, commitId))
            .whenComplete((commitId, throwable) -> {
                if (throwable != null) {
                    log.error("Write coordination failed for {}", request, throwable);
                    commitGatePort.releaseCommitLock(request);
                } else {
                    log.info("Write coordination completed successfully for {} with commit {}", 
                             request, commitId);
                }
            });
    }

    /**
     * Requests approval from all required regions for the commit.
     */
    private CompletableFuture<Result<String>> requestCommitApproval(CommitRequest request) {
        List<Region> requiredRegions = commitGatePort.getRequiredApprovalRegions(request.getTableId());
        log.debug("Requesting approval from {} regions for {}", requiredRegions.size(), request);
        
        return commitGatePort.requestCommitApproval(request);
    }

    /**
     * Validates that the approval result indicates success.
     */
    private CompletableFuture<Result<String>> validateApprovalResult(Result<String> result) {
        if (result.isSuccess()) {
            return CompletableFuture.completedFuture(result);
        } else {
            return CompletableFuture.failedFuture(
                new IllegalStateException("Commit approval failed: " + result.getErrorMessage().orElse("Unknown error")));
        }
    }

    /**
     * Commits the metadata to the global catalog once approval is received.
     */
    private CompletableFuture<CommitId> commitToGlobalCatalog(CommitRequest request) {
        try {
            CommitId commitId = catalogPort.commitMetadata(request);
            log.debug("Successfully committed metadata to global catalog: {}", commitId);
            return CompletableFuture.completedFuture(commitId);
        } catch (Exception e) {
            log.error("Failed to commit metadata to global catalog for {}", request, e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Triggers replication of the committed changes to all regions.
     */
    private CompletableFuture<CommitId> triggerReplication(CommitRequest request, CommitId commitId) {
        List<Region> targetRegions = registryPort.getTableRegions(request.getTableId());
        
        // Create metadata sync events for all regions except source
        targetRegions.stream()
            .filter(region -> !region.equals(request.getSourceRegion()))
            .forEach(targetRegion -> {
                try {
                    TableMetadata metadata = new TableMetadata(
                        request.getTableId(),
                        commitId,
                        request.getSourceRegion(),
                        request.getRequestTime(),
                        request.getNewDataFiles(),
                        request.getUpdatedSchema()
                    );
                    
                    SyncEvent metadataEvent = syncPort.createMetadataSyncEvent(metadata, targetRegion);
                    syncPort.publishSyncEvent(metadataEvent);
                    
                    if (!request.getNewDataFiles().isEmpty()) {
                        SyncEvent dataEvent = syncPort.createDataSyncEvent(
                            metadata, request.getNewDataFiles(), targetRegion);
                        syncPort.publishSyncEvent(dataEvent);
                    }
                    
                    log.debug("Created sync events for region {} and commit {}", 
                             targetRegion, commitId);
                } catch (Exception e) {
                    log.error("Failed to create sync events for region {} and commit {}", 
                             targetRegion, commitId, e);
                }
            });

        // Release the commit lock
        commitGatePort.releaseCommitLock(request);
        
        return CompletableFuture.completedFuture(commitId);
    }

    /**
     * Creates a new table in the catalog and registers it across regions.
     */
    public void createTable(TableId tableId, String schema, Region sourceRegion) {
        log.info("Creating table {} in region {}", tableId, sourceRegion);
        
        try {
            catalogPort.createTable(tableId, schema, sourceRegion);
            
            // Register table location in the source region
            String dataPath = generateTableDataPath(tableId);
            registryPort.registerTableLocation(tableId, sourceRegion, dataPath);
            
            log.info("Successfully created table {} in region {}", tableId, sourceRegion);
        } catch (Exception e) {
            log.error("Failed to create table {} in region {}", tableId, sourceRegion, e);
            throw e;
        }
    }

    /**
     * Generates a standard data path for a new table.
     */
    private String generateTableDataPath(TableId tableId) {
        return String.format("/data/%s/%s/%s", 
                           tableId.namespace(), 
                           tableId.name(), 
                           UUID.randomUUID());
    }
}