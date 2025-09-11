package com.streamfirst.iceberg.hybrid.ports;

import com.streamfirst.iceberg.hybrid.domain.CommitApproval;
import com.streamfirst.iceberg.hybrid.domain.CommitRequest;
import com.streamfirst.iceberg.hybrid.domain.Region;
import com.streamfirst.iceberg.hybrid.domain.Result;
import com.streamfirst.iceberg.hybrid.domain.TableId;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

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
    CompletableFuture<Result<String>> requestCommitApproval(CommitRequest request);
    
    /**
     * Records approval for a commit from a specific region.
     * 
     * @param request the commit request being approved
     * @param approvingRegion the region providing approval
     * @return result indicating success or failure of approval recording
     */
    Result<String> approveCommit(CommitRequest request, Region approvingRegion);
    
    /**
     * Records rejection for a commit from a specific region.
     * This will cause the entire commit to be rejected.
     * 
     * @param request the commit request being rejected
     * @param rejectingRegion the region rejecting the commit
     * @param reason the reason for rejection
     * @return result indicating success or failure of rejection recording
     */
    Result<String> rejectCommit(CommitRequest request, Region rejectingRegion, String reason);
    
    /**
     * Gets commit approvals matching the specified criteria.
     * Provides flexible querying for complex commit gate scenarios.
     * 
     * @param predicate the filter criteria for commit approvals
     * @return list of matching commit approvals
     */
    List<CommitApproval> getCommitApprovals(Predicate<CommitApproval> predicate);
    
    /**
     * Gets all approvals for a specific commit request.
     * 
     * @param request the commit request to get approvals for
     * @return list of approvals from different regions
     */
    default List<CommitApproval> getApprovals(CommitRequest request) {
        return getCommitApprovals(approval -> approval.request().equals(request));
    }
    
    /**
     * Gets all pending commit requests for a specific region.
     * Used by regional commit gate workers to process their approval queue.
     * 
     * @param region the region to get pending commits for
     * @return list of commit requests awaiting approval from this region
     */
    default List<CommitRequest> getPendingCommits(Region region) {
        return getCommitApprovals(approval -> 
            approval.region().equals(region) && 
            approval.status() == CommitApproval.ApprovalStatus.PENDING)
            .stream()
            .map(CommitApproval::request)
            .distinct()
            .toList();
    }
    
    /**
     * Gets all pending commit requests for a specific table.
     * 
     * @param tableId the table to get pending commits for
     * @return list of commit requests for this table
     */
    default List<CommitRequest> getPendingCommits(TableId tableId) {
        return getCommitApprovals(approval -> 
            approval.request().getTableId().equals(tableId) && 
            approval.status() == CommitApproval.ApprovalStatus.PENDING)
            .stream()
            .map(CommitApproval::request)
            .distinct()
            .toList();
    }
    
    /**
     * Gets commit requests matching the specified criteria.
     * Provides flexible querying for complex commit gate scenarios.
     * 
     * @param predicate the filter criteria for commit requests
     * @return list of matching commit requests
     */
    default List<CommitRequest> getCommitRequests(Predicate<CommitRequest> predicate) {
        return getCommitApprovals(approval -> predicate.test(approval.request()))
            .stream()
            .map(CommitApproval::request)
            .distinct()
            .toList();
    }
    
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