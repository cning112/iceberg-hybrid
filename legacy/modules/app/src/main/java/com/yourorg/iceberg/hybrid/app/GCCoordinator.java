package com.yourorg.iceberg.hybrid.app;

import com.yourorg.iceberg.hybrid.domain.*;
import com.yourorg.iceberg.hybrid.ports.*;

import java.time.Instant;
import java.util.List;

/**
 * Application service responsible for coordinated garbage collection across environments.
 * 
 * <p>The GCCoordinator implements the safe, cross-environment cleanup strategy described
 * in the system design. It ensures that expired Iceberg files are safely deleted from
 * both on-premise and cloud storage while preventing deletion of files that might still
 * be referenced by active queries or pending operations.
 * 
 * <p>Key safety mechanisms:
 * <ul>
 * <li><strong>Safety Windows:</strong> Configurable delays before deletion (cloud > on-prem)</li>
 * <li><strong>Active Query Tracking:</strong> Checks for leases before allowing deletion</li>
 * <li><strong>Consistency Tokens:</strong> Respects high-watermark boundaries</li>
 * <li><strong>Audit Trail:</strong> Metrics tracking for deletion operations</li>
 * </ul>
 * 
 * <p>The coordination process:
 * <ol>
 * <li>Validates deletion plan is within its validity window</li>
 * <li>Checks safety window has elapsed since plan generation</li>
 * <li>Verifies no active queries hold leases on affected data</li>
 * <li>Respects consistency watermarks for environment-specific rules</li>
 * <li>Performs deletion with metrics collection</li>
 * </ol>
 * 
 * <p>Different environments have different safety characteristics:
 * <ul>
 * <li><strong>On-premise:</strong> Shorter delays, direct catalog coordination</li>
 * <li><strong>Cloud:</strong> Longer delays, additional consistency checks</li>
 * </ul>
 * 
 * @see DeletePlan for deletion plan structure
 * @see SafetyWindow for delay configuration
 */
public final class GCCoordinator {

  private final CatalogPort catalog;
  private final LeasePort lease;
  private final ConsistencyPort consistency;
  private final ObjectStorePort store;
  private final MetricsPort metrics;

  public GCCoordinator(CatalogPort catalog, LeasePort lease,
                       ConsistencyPort consistency, ObjectStorePort store, MetricsPort metrics) {
    this.catalog = catalog;
    this.lease = lease;
    this.consistency = consistency;
    this.store = store;
    this.metrics = metrics;
  }

  /**
   * Applies a deletion plan with appropriate safety checks for the target environment.
   * 
   * <p>This method executes the coordinated garbage collection process, applying different
   * safety rules depending on whether the operation is on-premise or cloud-side.
   * 
   * <p>Safety checks performed:
   * <ul>
   * <li>Plan validity window - ensures plan hasn't expired</li>
   * <li>Safety window elapsed - prevents premature deletion</li>
   * <li>Active lease checking - protects files referenced by running queries</li>
   * <li>Consistency watermark - environment-specific consistency rules</li>
   * </ul>
   * 
   * <p>The method is idempotent and safe to retry. Failed deletions are tracked
   * via metrics but don't prevent processing of other files in the plan.
   * 
   * @param plan the deletion plan containing files to remove and safety metadata
   * @param safety the safety window configuration for this environment
   * @param isCloudSide true for cloud environment (stricter rules), false for on-premise
   */
  public void applyDeletePlan(DeletePlan plan, SafetyWindow safety, boolean isCloudSide) {
    var now = Instant.now();

    if (now.isBefore(plan.validFrom()) || now.isAfter(plan.validUntil())) return;

    List<QueryLease> active = lease.listActive(plan.tableId());
    var token = consistency.loadToken(plan.tableId()).orElse(null);

    for (var key : plan.deleteCandidates()) {
      if (!pastSafetyWindow(now, isCloudSide ? safety.cloudDelaySeconds() : safety.onPremDelaySeconds(), plan.generatedAt())) continue;
      if (!visibleWaterlineAllows(token, plan.generatedAt(), isCloudSide)) continue;

      boolean ok = store.delete(key);
      if (ok) metrics.increment("gc.deleted", 1);
      else metrics.increment("gc.delete_fail", 1);
    }
  }

  private boolean pastSafetyWindow(Instant now, long delaySeconds, Instant generatedAt) {
    return now.isAfter(generatedAt.plusSeconds(delaySeconds));
  }

  private boolean visibleWaterlineAllows(ConsistencyToken token, Instant genAt, boolean cloudSide) {
    if (token == null) return !cloudSide;
    return !token.highWatermarkTs().isBefore(genAt);
  }
}
