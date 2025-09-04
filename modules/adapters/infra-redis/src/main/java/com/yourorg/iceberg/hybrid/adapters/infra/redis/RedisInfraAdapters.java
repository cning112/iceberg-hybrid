package com.yourorg.iceberg.hybrid.adapters.infra.redis;

import com.yourorg.iceberg.hybrid.domain.*;
import com.yourorg.iceberg.hybrid.ports.*;

import java.util.*;

public final class RedisInfraAdapters {

  public static final class InMemoryLeaseAdapter implements LeasePort {
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

  public static final class InMemoryConsistencyAdapter implements ConsistencyPort {
    private final Map<TableId, ConsistencyToken> tokens = new HashMap<>();
    @Override public java.util.Optional<ConsistencyToken> loadToken(TableId tableId) {
      return java.util.Optional.ofNullable(tokens.get(tableId));
    }
    @Override public void saveToken(TableId tableId, ConsistencyToken token) {
      tokens.put(tableId, token);
    }
  }
}
