package com.streamfirst.iceberg.hybrid.ports;

public interface MetricsPort {
  void increment(String name, long delta);
  void observe(String name, double value);
}
