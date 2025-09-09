package com.streamfirst.iceberg.hybrid.adapters.infra.kafka;

import com.streamfirst.iceberg.hybrid.ports.EventBusPort;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public final class KafkaEventBusAdapter implements EventBusPort {
  // Stub: in-memory pub/sub
  private final Map<String, List<EventHandler>> subs = new HashMap<>();
  @Override public void publish(String eventType, Object payload) {
    for (var h : subs.getOrDefault(eventType, List.of())) h.handle(payload);
  }
  @Override public void subscribe(String eventType, EventHandler handler) {
    subs.computeIfAbsent(eventType, k -> new CopyOnWriteArrayList<>()).add(handler);
  }
}
