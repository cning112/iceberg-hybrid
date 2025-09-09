package com.streamfirst.iceberg.hybrid.ports;

import com.streamfirst.iceberg.hybrid.domain.CommitRequest;
import com.streamfirst.iceberg.hybrid.domain.Region;
import com.streamfirst.iceberg.hybrid.domain.TableId;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Port for regional commit gate coordination.
 * Ensures write consistency by requiring approval from all regions before committing.
 * Implements the distributed consensus mechanism for geo-distributed writes.
 */
public interface CommitGatePort {
    
    /**
     * Requests commit approval from all required regions.
     * This initiates the distributed commit protocol.
     * 
     * @param request the commit request to approve
     * @return a future that completes when all regions have responded
     */
    CompletableFuture<SyncResult> requestCommitApproval(CommitRequest request);
    
    /**
     * Records approval for a commit from a specific region.
     * 
     * @param request the commit request being approved
     * @param approvingRegion the region providing approval
     * @return result indicating success or failure of approval recording
     */
    SyncResult approveCommit(CommitRequest request, Region approvingRegion);
    
    /**
     * Records rejection for a commit from a specific region.
     * This will cause the entire commit to be rejected.
     * 
     * @param request the commit request being rejected
     * @param rejectingRegion the region rejecting the commit
     * @param reason the reason for rejection
     * @return result indicating success or failure of rejection recording
     */
    SyncResult rejectCommit(CommitRequest request, Region rejectingRegion, String reason);
    
    /**
     * Gets all pending commit requests for a specific region.
     * Used by regional commit gate workers to process their approval queue.
     * 
     * @param region the region to get pending commits for
     * @return list of commit requests awaiting approval from this region
     */
    List<CommitRequest> getPendingCommits(Region region);
    
    /**
     * Gets all pending commit requests for a specific table.
     * 
     * @param tableId the table to get pending commits for
     * @return list of commit requests for this table
     */
    List<CommitRequest> getPendingCommits(TableId tableId);
    
    /**
     * Checks if a commit has received approval from all required regions.
     * 
     * @param request the commit request to check
     * @return true if approved by all regions, false otherwise
     */
    boolean isCommitApproved(CommitRequest request);
    
    /**
     * Releases the commit lock, allowing other commits to proceed.
     * Called after successful commit or when cleaning up failed commits.
     * 
     * @param request the commit request to release
     */
    void releaseCommitLock(CommitRequest request);
    
    /**
     * Gets the list of regions that must approve commits for a table.
     * This depends on the table's replication configuration.
     * 
     * @param tableId the table identifier
     * @return list of regions that must approve commits for this table
     */
    List<Region> getRequiredApprovalRegions(TableId tableId);
}