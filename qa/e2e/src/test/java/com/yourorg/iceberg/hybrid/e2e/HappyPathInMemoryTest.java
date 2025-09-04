package com.yourorg.iceberg.hybrid.e2e;

import com.yourorg.iceberg.hybrid.adapters.catalog.nessie.NessieCatalogAdapter;
import com.yourorg.iceberg.hybrid.adapters.infra.redis.RedisInfraAdapters.InMemoryConsistencyAdapter;
import com.yourorg.iceberg.hybrid.adapters.infra.redis.RedisInfraAdapters.InMemoryLeaseAdapter;
import com.yourorg.iceberg.hybrid.adapters.inventory.s3.S3InventoryAdapter;
import com.yourorg.iceberg.hybrid.adapters.storage.s3.S3ObjectStoreAdapter;
import com.yourorg.iceberg.hybrid.app.*;
import com.yourorg.iceberg.hybrid.domain.*;
import com.yourorg.iceberg.hybrid.ports.*;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class HappyPathInMemoryTest {

  @Test
  void commit_replicate_verify_promote_and_route() {
    // Setup adapters (in-memory stubs)
    CatalogPort onPrem = new NessieCatalogAdapter();
    CatalogPort cloud = new NessieCatalogAdapter();
    ObjectStorePort cloudStore = new S3ObjectStoreAdapter();
    InventoryPort inventory = new S3InventoryAdapter();
    var lease = new InMemoryLeaseAdapter();
    var token = new InMemoryConsistencyAdapter();

    var table = new TableId("demo", "orders");
    var snapId = new SnapshotId("s-1", 1L, Instant.now());

    // Prepare a snapshot with 2 files
    var f1 = new FileRef("s3://cloud-bucket/data/part-000.parquet", ContentType.DATA, "p=1", 123L, "etag1", Instant.now());
    var f2 = new FileRef("s3://cloud-bucket/data/part-001.parquet", ContentType.DATA, "p=1", 456L, "etag2", Instant.now());
    var manifest = new Manifest("s3://cloud-bucket/manifest-1.avro", List.of(f1, f2));
    var snapshot = new CatalogPort.Snapshot(snapId, List.of(manifest), Map.of());

    // On-prem commit (stubbed)
    onPrem.commitSnapshot(table, snapshot, java.util.Optional.empty());

    // Plan replication (inventory empty => need to copy)
    var planner = new ReplicationPlanner(onPrem, cloud, inventory, cloudStore);
    var plan = planner.plan(table, snapId, inventory.loadIndex("cloud-bucket", "data/", "latest"));
    assertThat(plan.objectsToCopy()).containsExactlyInAnyOrder(f1.path(), f2.path());

    // "Copy": simulate content presence
    if (cloudStore instanceof S3ObjectStoreAdapter s) {
      s.put(f1.path(), f1.size(), f1.etag());
      s.put(f2.path(), f2.size(), f2.etag());
    }

    // Cloud "commit" (shadow -> visible via reconciler)
    cloud.commitSnapshot(table, snapshot, java.util.Optional.empty());

    var reconciler = new StateReconciler(cloud, cloudStore);
    reconciler.verifyAndPromote(table, snapId, Instant.now());

    // Save cloud visibility watermark
    token.saveToken(table, new ConsistencyToken(snapId.commitTs(), snapId.sequenceNumber(), "inv-v0"));

    // Route a read
    var router = new ReadRouter(token);
    var route = router.route(table, snapId, ReadRouter.RoutingPolicy.MEET_WATERMARK);
    assertThat(route.target()).isEqualTo(ReadRouter.Target.CLOUD);
  }
}
