package com.streamfirst.iceberg.hybrid.adapters;

import com.streamfirst.iceberg.hybrid.domain.CommitRequest;
import com.streamfirst.iceberg.hybrid.domain.Region;
import com.streamfirst.iceberg.hybrid.domain.TableId;
import com.streamfirst.iceberg.hybrid.ports.CommitGatePort;
import com.streamfirst.iceberg.hybrid.ports.SyncResult;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of CommitGatePort for testing and development.
 * Provides simple commit approval tracking using in-memory collections.
 * Simulates distributed consensus by tracking approvals from different regions.
 */
@Slf4j
public class InMemoryCommitGateAdapter implements CommitGatePort {
    
    private final Map<CommitRequest, Set<Region>> commitApprovals = new ConcurrentHashMap<>();
    private final Map<CommitRequest, Set<Region>> commitRejections = new ConcurrentHashMap<>();
    private final Map<CommitRequest, String> rejectionReasons = new ConcurrentHashMap<>();
    private final Map<Region, List<CommitRequest>> pendingCommitsByRegion = new ConcurrentHashMap<>();
    private final Map<TableId, List<Region>> requiredRegionsByTable = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<SyncResult> requestCommitApproval(CommitRequest request) {
        log.info("Requesting commit approval for {}", request);
        
        // Get required regions for this table
        List<Region> requiredRegions = getRequiredApprovalRegions(request.getTableId());
        
        if (requiredRegions.isEmpty()) {
            log.warn("No regions required for table {} - auto-approving", request.getTableId());
            return CompletableFuture.completedFuture(SyncResult.success("Auto-approved (no regions required)"));
        }
        
        // Initialize approval tracking
        commitApprovals.put(request, ConcurrentHashMap.newKeySet());
        commitRejections.put(request, ConcurrentHashMap.newKeySet());
        
        // Add to pending queues for each required region
        for (Region region : requiredRegions) {
            pendingCommitsByRegion.computeIfAbsent(region, k -> new ArrayList<>()).add(request);
        }
        
        log.debug("Added commit request to pending queues for {} regions", requiredRegions.size());
        
        // For this simple implementation, simulate instant approval from all regions
        // In a real implementation, this would wait for actual regional responses
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Simulate some processing time
                Thread.sleep(100);
                
                // Auto-approve from all regions for testing
                for (Region region : requiredRegions) {
                    approveCommit(request, region);
                }
                
                if (isCommitApproved(request)) {
                    return SyncResult.success("Approved by all required regions");
                } else {
                    return SyncResult.failure("Failed to get approval from all regions");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return SyncResult.failure("Approval request interrupted", "INTERRUPTED");
            } catch (Exception e) {
                log.error("Error during commit approval process", e);
                return SyncResult.failure("Approval process failed: " + e.getMessage(), "APPROVAL_ERROR");
            }
        });
    }

    @Override
    public SyncResult approveCommit(CommitRequest request, Region approvingRegion) {
        log.debug("Region {} approving commit for {}", approvingRegion, request.getTableId());
        
        // Check if this region can approve this commit
        List<Region> requiredRegions = getRequiredApprovalRegions(request.getTableId());
        if (!requiredRegions.contains(approvingRegion)) {
            String message = "Region " + approvingRegion + " is not required for table " + request.getTableId();
            log.warn(message);
            return SyncResult.failure(message, "INVALID_REGION");
        }
        
        // Check if already rejected
        Set<Region> rejections = commitRejections.get(request);
        if (rejections != null && rejections.contains(approvingRegion)) {
            String message = "Region " + approvingRegion + " already rejected this commit";
            log.warn(message);
            return SyncResult.failure(message, "ALREADY_REJECTED");
        }
        
        // Add approval
        Set<Region> approvals = commitApprovals.computeIfAbsent(request, k -> ConcurrentHashMap.newKeySet());
        approvals.add(approvingRegion);
        
        // Remove from pending queue
        List<CommitRequest> pendingForRegion = pendingCommitsByRegion.get(approvingRegion);
        if (pendingForRegion != null) {
            pendingForRegion.remove(request);
        }
        
        log.info("Region {} approved commit for {} ({}/{} approvals)", 
                approvingRegion, request.getTableId(), approvals.size(), requiredRegions.size());
        
        return SyncResult.success("Approval recorded from " + approvingRegion);
    }

    @Override
    public SyncResult rejectCommit(CommitRequest request, Region rejectingRegion, String reason) {
        log.warn("Region {} rejecting commit for {}: {}", 
                rejectingRegion, request.getTableId(), reason);
        
        // Check if this region can reject this commit
        List<Region> requiredRegions = getRequiredApprovalRegions(request.getTableId());
        if (!requiredRegions.contains(rejectingRegion)) {
            String message = "Region " + rejectingRegion + " is not required for table " + request.getTableId();
            log.warn(message);
            return SyncResult.failure(message, "INVALID_REGION");
        }
        
        // Add rejection
        Set<Region> rejections = commitRejections.computeIfAbsent(request, k -> ConcurrentHashMap.newKeySet());
        rejections.add(rejectingRegion);
        rejectionReasons.put(request, reason);
        
        // Remove from pending queues for all regions (commit is now rejected)
        for (Region region : requiredRegions) {
            List<CommitRequest> pendingForRegion = pendingCommitsByRegion.get(region);
            if (pendingForRegion != null) {
                pendingForRegion.remove(request);
            }
        }
        
        log.info("Region {} rejected commit for {} - commit is now rejected", 
                rejectingRegion, request.getTableId());
        
        return SyncResult.success("Rejection recorded from " + rejectingRegion);
    }

    @Override
    public List<CommitRequest> getPendingCommits(Region region) {
        List<CommitRequest> pending = pendingCommitsByRegion.get(region);
        if (pending == null) {
            log.debug("No pending commits for region {}", region);
            return List.of();
        }
        
        List<CommitRequest> result = new ArrayList<>(pending);
        log.debug("Found {} pending commits for region {}", result.size(), region);
        return result;
    }

    @Override
    public List<CommitRequest> getPendingCommits(TableId tableId) {
        List<CommitRequest> pending = new ArrayList<>();
        
        for (List<CommitRequest> regionPending : pendingCommitsByRegion.values()) {
            for (CommitRequest request : regionPending) {
                if (request.getTableId().equals(tableId) && !pending.contains(request)) {
                    pending.add(request);
                }
            }
        }
        
        log.debug("Found {} pending commits for table {}", pending.size(), tableId);
        return pending;
    }

    @Override
    public boolean isCommitApproved(CommitRequest request) {
        Set<Region> approvals = commitApprovals.get(request);
        Set<Region> rejections = commitRejections.get(request);
        List<Region> requiredRegions = getRequiredApprovalRegions(request.getTableId());
        
        // If any region rejected, commit is not approved
        if (rejections != null && !rejections.isEmpty()) {
            log.debug("Commit for {} is rejected by regions: {}", 
                     request.getTableId(), rejections);
            return false;
        }
        
        // Check if all required regions have approved
        boolean approved = approvals != null && 
                          approvals.containsAll(requiredRegions) && 
                          approvals.size() == requiredRegions.size();
        
        log.debug("Commit for {} is approved: {} ({}/{} regions)", 
                 request.getTableId(), approved, 
                 approvals != null ? approvals.size() : 0, requiredRegions.size());
        
        return approved;
    }

    @Override
    public void releaseCommitLock(CommitRequest request) {
        log.debug("Releasing commit lock for {}", request.getTableId());
        
        // Clean up approval tracking
        commitApprovals.remove(request);
        commitRejections.remove(request);
        rejectionReasons.remove(request);
        
        // Remove from all pending queues
        for (List<CommitRequest> pending : pendingCommitsByRegion.values()) {
            pending.remove(request);
        }
        
        log.info("Released commit lock for {}", request.getTableId());
    }

    @Override
    public List<Region> getRequiredApprovalRegions(TableId tableId) {
        List<Region> required = requiredRegionsByTable.get(tableId);
        if (required == null) {
            log.debug("No specific regions required for table {} - using empty list", tableId);
            return List.of();
        }
        
        log.debug("Table {} requires approval from {} regions: {}", 
                 tableId, required.size(), required);
        return new ArrayList<>(required);
    }

    /**
     * Configures which regions are required for commit approval for a table.
     * Used for testing setup.
     */
    public void setRequiredApprovalRegions(TableId tableId, List<Region> regions) {
        log.info("Setting required approval regions for table {}: {}", tableId, regions);
        requiredRegionsByTable.put(tableId, new ArrayList<>(regions));
    }

    /**
     * Clears all commit gate data. Useful for testing.
     */
    public void clear() {
        log.info("Clearing all commit gate data");
        commitApprovals.clear();
        commitRejections.clear();
        rejectionReasons.clear();
        pendingCommitsByRegion.clear();
        requiredRegionsByTable.clear();
    }

    /**
     * Gets commit gate statistics for monitoring.
     */
    public Map<String, Integer> getCommitGateStats() {
        Map<String, Integer> stats = new HashMap<>();
        
        stats.put("pending_commits", pendingCommitsByRegion.values().stream()
                                                          .mapToInt(List::size)
                                                          .sum());
        stats.put("tracked_commits", commitApprovals.size());
        stats.put("approved_commits", (int) commitApprovals.entrySet().stream()
                                                          .filter(entry -> isCommitApproved(entry.getKey()))
                                                          .count());
        stats.put("rejected_commits", commitRejections.size());
        
        return stats;
    }
}