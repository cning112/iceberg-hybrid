package com.yourorg.iceberg.hybrid.ports;

public interface MetricsPort {
  void increment(String name, long delta);
  void observe(String name, double value);
}
