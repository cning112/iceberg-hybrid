package com.yourorg.iceberg.hybrid.app;

import com.yourorg.iceberg.hybrid.domain.*;
import com.yourorg.iceberg.hybrid.ports.*;

import java.time.Instant;
import java.util.List;

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
