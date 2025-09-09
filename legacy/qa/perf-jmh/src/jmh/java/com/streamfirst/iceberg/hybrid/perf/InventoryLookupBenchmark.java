package com.streamfirst.iceberg.hybrid.perf;

import com.streamfirst.iceberg.hybrid.adapters.inventory.s3.S3InventoryAdapter;
import com.streamfirst.iceberg.hybrid.ports.InventoryPort;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class InventoryLookupBenchmark {

  private S3InventoryAdapter.Index index;
  private final InventoryPort inventory = new S3InventoryAdapter();

  @Setup
  public void setup() {
    index = new S3InventoryAdapter.Index("bench");
    for (int i = 0; i < 100_000; i++) {
      index.add("s3://bucket/data/file-" + i + ".parquet");
    }
  }

  @Benchmark
  public boolean contains_hit() {
    return inventory.contains(index, "s3://bucket/data/file-4242.parquet", null, 0L);
  }

  @Benchmark
  public boolean contains_miss() {
    return inventory.contains(index, "s3://bucket/data/missing.parquet", null, 0L);
  }
}
