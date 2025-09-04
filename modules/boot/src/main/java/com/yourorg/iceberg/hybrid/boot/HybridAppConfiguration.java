package com.yourorg.iceberg.hybrid.boot;

import com.yourorg.iceberg.hybrid.adapters.catalog.nessie.NessieCatalogStub;
import com.yourorg.iceberg.hybrid.adapters.infra.redis.RedisInfraAdapters.InMemoryConsistencyStub;
import com.yourorg.iceberg.hybrid.adapters.inventory.s3.S3InventoryStub;
import com.yourorg.iceberg.hybrid.adapters.storage.s3.S3ObjectStoreStub;
import com.yourorg.iceberg.hybrid.app.*;
import com.yourorg.iceberg.hybrid.domain.*;
import com.yourorg.iceberg.hybrid.ports.*;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Main application configuration for wiring and demonstrating the replication flow.
 */
@Configuration
public class HybridAppConfiguration {

    // --- Adapter Beans (Simulating On-Prem and Cloud environments) ---

    @Bean
    public CatalogPort onPremCatalog() {
        System.out.println("Creating On-Prem Catalog Bean (in-memory)");
        return new NessieCatalogStub();
    }

    @Bean
    public CatalogPort cloudCatalog() {
        System.out.println("Creating Cloud Catalog Bean (in-memory)");
        return new NessieCatalogStub();
    }

    @Bean
    public ObjectStorePort cloudStore() {
        return new S3ObjectStoreStub();
    }

    @Bean
    public InventoryPort cloudInventory() {
        return new S3InventoryStub();
    }

    @Bean
    public ConsistencyPort consistencyService() {
        return new InMemoryConsistencyStub();
    }

    // --- Application Service Beans (The core logic) ---

    @Bean
    public ReplicationPlanner replicationPlanner(CatalogPort onPremCatalog, CatalogPort cloudCatalog, InventoryPort cloudInventory, ObjectStorePort cloudStore) {
        // Correctly inject two separate catalog instances
        return new ReplicationPlanner(onPremCatalog, cloudCatalog, cloudInventory, cloudStore);
    }

    @Bean
    public StateReconciler stateReconciler(CatalogPort cloudCatalog, ObjectStorePort cloudStore) {
        return new StateReconciler(cloudCatalog, cloudStore);
    }

    @Bean
    public ReadRouter readRouter(ConsistencyPort consistencyService) {
        return new ReadRouter(consistencyService);
    }


    // --- Execution Logic (A demo runner) ---

    @Bean
    public CommandLineRunner demo(
            ReplicationPlanner planner,
            StateReconciler reconciler,
            ReadRouter router,
            CatalogPort onPremCatalog, // Inject the on-prem catalog
            CatalogPort cloudCatalog,  // Inject the cloud catalog
            ObjectStorePort cloudStore,
            InventoryPort cloudInventory,
            ConsistencyPort consistencyService
    ) {
        return args -> {
            System.out.println("\n--- Starting Iceberg Hybrid Replication Demo ---");

            // 1. Setup: Define table and create a dummy snapshot
            var table = new TableId("demo", "orders");
            var snapId = new SnapshotId("s-1", 1L, Instant.now());
            var f1 = new FileRef("s3://cloud-bucket/data/part-000.parquet", ContentType.DATA, "p=1", 123L, "etag1", Instant.now());
            var f2 = new FileRef("s3://cloud-bucket/data/part-001.parquet", ContentType.DATA, "p=1", 456L, "etag2", Instant.now());
            var manifest = new Manifest("s3://cloud-bucket/manifest-1.avro", List.of(f1, f2));
            var snapshot = new CatalogPort.Snapshot(snapId, List.of(manifest), Map.of());
            System.out.println("STEP 1: A new snapshot with 2 files has been created on-prem.");

            // 2. On-prem Commit: Add the snapshot to the ON-PREM catalog
            onPremCatalog.commitSnapshot(table, snapshot, Optional.empty());
            System.out.println("STEP 2: The new snapshot has been committed to the on-prem Source-of-Truth catalog.");

            // 3. Plan Replication: Calculate which files need to be copied
            var plan = planner.plan(table, snapId, cloudInventory.loadIndex("cloud-bucket", "data/", "latest"));
            System.out.println("STEP 3: Replication planner has run. Plan result: " + plan.objectsToCopy().size() + " objects to copy.");
            System.out.println("  -> Files to copy: " + plan.objectsToCopy());
            if (plan.objectsToCopy().size() != 2) throw new AssertionError("Expected 2 objects to copy, but got " + plan.objectsToCopy().size());

            // 4. "Copy": Simulate files being physically copied to the cloud store
            if (cloudStore instanceof S3ObjectStoreStub s) {
                s.put(f1.path(), f1.size(), f1.etag());
                s.put(f2.path(), f2.size(), f2.etag());
            }
            System.out.println("STEP 4: The 2 files have been 'copied' to the cloud object store.");

            // 5. Cloud "Commit": The snapshot metadata is now also written to the CLOUD catalog (but not yet visible)
            cloudCatalog.commitSnapshot(table, snapshot, Optional.empty());
            System.out.println("STEP 5: Snapshot metadata has been written to the cloud catalog.");

            // 6. Verify & Promote: The reconciler verifies file presence and makes the snapshot visible
            reconciler.verifyAndPromote(table, snapId, Instant.now());
            System.out.println("STEP 6: StateReconciler has verified files and promoted the snapshot to be visible in the cloud.");

            // 7. Update Watermark: The consistency service is updated with the latest visible snapshot timestamp
            consistencyService.saveToken(table, new ConsistencyToken(snapId.commitTs(), snapId.sequenceNumber(), "inv-v0"));
            System.out.println("STEP 7: Consistency watermark has been updated.");

            // 8. Route a Read: A client asks where to read the data from
            var route = router.route(table, snapId, ReadRouter.RoutingPolicy.MEET_WATERMARK);
            System.out.println("STEP 8: A client requests to read the data for snapshot " + snapId.sequenceNumber() + "...");
            System.out.println("  -> ReadRouter directs the client to: " + route.target());
            if (route.target() != ReadRouter.Target.CLOUD) throw new AssertionError("Expected route target to be CLOUD");

            System.out.println("\n--- Demo Finished Successfully ---");
        };
    }
}
