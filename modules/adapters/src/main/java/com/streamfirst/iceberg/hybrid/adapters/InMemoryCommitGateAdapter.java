package com.streamfirst.iceberg.hybrid.adapters;

import com.streamfirst.iceberg.hybrid.domain.CommitApproval;
import com.streamfirst.iceberg.hybrid.domain.CommitRequest;
import com.streamfirst.iceberg.hybrid.domain.Region;
import com.streamfirst.iceberg.hybrid.domain.Result;
import com.streamfirst.iceberg.hybrid.domain.TableId;
import com.streamfirst.iceberg.hybrid.ports.CommitGatePort;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * In-memory implementation of CommitGatePort for testing and development.
 * Provides simple commit approval tracking using in-memory collections.
 * Simulates distributed consensus by tracking approvals from different regions.
 */
@Slf4j
public class InMemoryCommitGateAdapter implements CommitGatePort {
    
    private final Map<CommitRequest, List<CommitApproval>> commitApprovals = new ConcurrentHashMap<>();
    private final Map<Region, List<CommitRequest>> pendingCommitsByRegion = new ConcurrentHashMap<>();
    private final Map<TableId, Set<Region>> requiredRegionsByTable = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<Result<String>> requestCommitApproval(CommitRequest request) {
        log.info("Requesting commit approval for {}", request);
        
        // Get required regions for this table
        Set<Region> requiredRegions = Set.copyOf(getRequiredApprovalRegions(request.getTableId()));
        
        if (requiredRegions.isEmpty()) {
            log.warn("No regions required for table {} - auto-approving", request.getTableId());
            return CompletableFuture.completedFuture(Result.success("Auto-approved (no regions required)"));
        }
        
        // Initialize pending approvals for all required regions
        List<CommitApproval> approvals = new ArrayList<>();
        for (Region region : requiredRegions) {
            approvals.add(CommitApproval.pending(request, region));
        }
        commitApprovals.put(request, approvals);
        
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
                    return Result.success("Approved by all required regions");
                } else {
                    return Result.failure("Failed to get approval from all regions");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Result.failure("Approval request interrupted", "INTERRUPTED");
            } catch (Exception e) {
                log.error("Error during commit approval process", e);
                return Result.failure("Approval process failed: " + e.getMessage(), "APPROVAL_ERROR");
            }
        });
    }

    @Override
    public Result<String> approveCommit(CommitRequest request, Region approvingRegion) {
        log.debug("Region {} approving commit for {}", approvingRegion, request.getTableId());
        
        List<CommitApproval> approvals = commitApprovals.get(request);
        if (approvals == null) {
            String message = "No approval tracking found for commit request";
            log.warn(message);
            return Result.failure(message, "STATE_NOT_FOUND");
        }
        
        // Find and update the approval for this region
        boolean found = false;
        for (int i = 0; i < approvals.size(); i++) {
            CommitApproval approval = approvals.get(i);
            if (approval.region().equals(approvingRegion)) {
                if (approval.status() == CommitApproval.ApprovalStatus.REJECTED) {
                    return Result.failure("Region already rejected this commit", "ALREADY_REJECTED");
                }
                approvals.set(i, CommitApproval.approved(request, approvingRegion));
                found = true;
                break;
            }
        }
        
        if (!found) {
            return Result.failure("Region not required for this commit", "INVALID_REGION");
        }
        
        // Remove from pending queue
        List<CommitRequest> pendingForRegion = pendingCommitsByRegion.get(approvingRegion);
        if (pendingForRegion != null) {
            pendingForRegion.remove(request);
        }
        
        long approvedCount = approvals.stream()
            .filter(a -> a.status() == CommitApproval.ApprovalStatus.APPROVED)
            .count();
        
        log.info("Region {} approved commit for {} ({}/{} approvals)", 
                approvingRegion, request.getTableId(), approvedCount, approvals.size());
        
        return Result.success("Approval recorded from " + approvingRegion);
    }

    @Override
    public Result<String> rejectCommit(CommitRequest request, Region rejectingRegion, String reason) {
        log.warn("Region {} rejecting commit for {}: {}", 
                rejectingRegion, request.getTableId(), reason);
        
        List<CommitApproval> approvals = commitApprovals.get(request);
        if (approvals == null) {
            String message = "No approval tracking found for commit request";
            log.warn(message);
            return Result.failure(message, "STATE_NOT_FOUND");
        }
        
        // Find and update the approval for this region
        boolean found = false;
        for (int i = 0; i < approvals.size(); i++) {
            CommitApproval approval = approvals.get(i);
            if (approval.region().equals(rejectingRegion)) {
                approvals.set(i, CommitApproval.rejected(request, rejectingRegion, reason));
                found = true;
                break;
            }
        }
        
        if (!found) {
            return Result.failure("Region not required for this commit", "INVALID_REGION");
        }
        
        // Remove from pending queues for all regions (commit is now rejected)
        Set<Region> requiredRegions = Set.copyOf(getRequiredApprovalRegions(request.getTableId()));
        for (Region region : requiredRegions) {
            List<CommitRequest> pendingForRegion = pendingCommitsByRegion.get(region);
            if (pendingForRegion != null) {
                pendingForRegion.remove(request);
            }
        }
        
        log.info("Region {} rejected commit for {} - commit is now rejected", 
                rejectingRegion, request.getTableId());
        
        return Result.success("Rejection recorded from " + rejectingRegion);
    }

    @Override
    public List<CommitApproval> getCommitApprovals(Predicate<CommitApproval> predicate) {
        return commitApprovals.values().stream()
                .flatMap(List::stream)
                .filter(predicate)
                .toList();
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
        List<CommitApproval> approvals = commitApprovals.get(request);
        if (approvals == null) {
            return false;
        }
        
        // Check if any rejection exists
        boolean hasRejection = approvals.stream()
            .anyMatch(a -> a.status() == CommitApproval.ApprovalStatus.REJECTED);
        if (hasRejection) {
            return false;
        }
        
        // Check if all are approved
        return approvals.stream()
            .allMatch(a -> a.status() == CommitApproval.ApprovalStatus.APPROVED);
    }

    @Override
    public void releaseCommitLock(CommitRequest request) {
        log.debug("Releasing commit lock for {}", request.getTableId());
        
        // Clean up approval tracking
        commitApprovals.remove(request);
        
        // Remove from all pending queues
        for (List<CommitRequest> pending : pendingCommitsByRegion.values()) {
            pending.remove(request);
        }
        
        log.info("Released commit lock for {}", request.getTableId());
    }

    @Override
    public List<Region> getRequiredApprovalRegions(TableId tableId) {
        Set<Region> required = requiredRegionsByTable.get(tableId);
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
        requiredRegionsByTable.put(tableId, new HashSet<>(regions));
    }

    /**
     * Clears all commit gate data. Useful for testing.
     */
    public void clear() {
        log.info("Clearing all commit gate data");
        commitApprovals.clear();
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
        
        long approvedCommits = commitApprovals.entrySet().stream()
            .filter(entry -> entry.getValue().stream().allMatch(a -> a.status() == CommitApproval.ApprovalStatus.APPROVED))
            .count();
        stats.put("approved_commits", (int) approvedCommits);
        
        long rejectedCommits = commitApprovals.entrySet().stream()
            .filter(entry -> entry.getValue().stream().anyMatch(a -> a.status() == CommitApproval.ApprovalStatus.REJECTED))
            .count();
        stats.put("rejected_commits", (int) rejectedCommits);
        
        return stats;
    }
}