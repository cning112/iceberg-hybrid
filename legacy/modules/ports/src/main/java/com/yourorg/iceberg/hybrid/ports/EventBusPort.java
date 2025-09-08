package com.yourorg.iceberg.hybrid.ports;

public interface EventBusPort {
  void publish(String eventType, Object payload);
  void subscribe(String eventType, EventHandler handler);
  interface EventHandler { void handle(Object payload); }
}
