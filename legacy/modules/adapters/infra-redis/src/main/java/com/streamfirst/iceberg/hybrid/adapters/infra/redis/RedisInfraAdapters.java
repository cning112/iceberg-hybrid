package com.streamfirst.iceberg.hybrid.adapters.infra.redis;

import com.streamfirst.iceberg.hybrid.domain.*;
import com.streamfirst.iceberg.hybrid.ports.*;

import java.util.*;

public final class RedisInfraAdapters {

  public static final class InMemoryLeaseStub implements LeasePort {
    private final Map<String, QueryLease> leases = new HashMap<>();

    @Override
    public String create(TableId tableId, SnapshotId snapshotId, String holder, long ttlSeconds) {
      String id = UUID.randomUUID().toString();
      leases.put(id, new QueryLease(id, tableId, snapshotId, holder,
          java.time.Instant.now().plusSeconds(ttlSeconds)));
      return id;
    }

    @Override public void renew(String leaseId, long ttlSeconds) {
      leases.computeIfPresent(leaseId, (k, v) -> new QueryLease(
          v.leaseId(), v.tableId(), v.snapshotId(), v.holder(),
          java.time.Instant.now().plusSeconds(ttlSeconds)));
    }

    @Override public void release(String leaseId) { leases.remove(leaseId); }

    @Override public java.util.List<QueryLease> listActive(TableId tableId) {
      var now = java.time.Instant.now();
      return leases.values().stream()
          .filter(l -> l.tableId().equals(tableId) && l.expireAt().isAfter(now))
          .toList();
    }
  }

  public static final class InMemoryConsistencyStub implements ConsistencyPort {
    private final Map<TableId, ConsistencyToken> tokens = new HashMap<>();
    @Override public java.util.Optional<ConsistencyToken> loadToken(TableId tableId) {
      return java.util.Optional.ofNullable(tokens.get(tableId));
    }
    @Override public void saveToken(TableId tableId, ConsistencyToken token) {
      tokens.put(tableId, token);
    }
  }

  public static final class InMemoryMetricsStub implements MetricsPort {
    private final Map<String, Long> counters = new HashMap<>();
    private final Map<String, Double> observations = new HashMap<>();

    @Override 
    public void increment(String name, long delta) {
      counters.merge(name, delta, Long::sum);
      System.out.println("METRICS: Incremented '" + name + "' by " + delta + " (total: " + counters.get(name) + ")");
    }

    @Override 
    public void observe(String name, double value) {
      observations.put(name, value);
      System.out.println("METRICS: Observed '" + name + "' value: " + value);
    }

    public Map<String, Long> getCounters() { return new HashMap<>(counters); }
    public Map<String, Double> getObservations() { return new HashMap<>(observations); }
  }
}
