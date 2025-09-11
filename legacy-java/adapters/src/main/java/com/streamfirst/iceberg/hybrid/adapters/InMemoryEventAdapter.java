package com.streamfirst.iceberg.hybrid.adapters;

import com.streamfirst.iceberg.hybrid.ports.EventPort;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;

/**
 * In-memory implementation of EventPort for testing and development. Provides simple
 * publish/subscribe functionality using in-memory collections. Events are delivered synchronously
 * within the same JVM.
 */
@Slf4j
public class InMemoryEventAdapter implements EventPort {

  private final Map<String, List<Consumer<Object>>> topicSubscribers = new ConcurrentHashMap<>();
  private final Map<String, Map<Class<?>, List<Consumer<Object>>>> typedSubscribers =
      new ConcurrentHashMap<>();
  private final Map<String, String> subscriptionToTopic = new ConcurrentHashMap<>();
  private final Map<String, Consumer<Object>> subscriptionToHandler = new ConcurrentHashMap<>();
  private final AtomicLong subscriptionCounter = new AtomicLong(1);
  private volatile boolean connected = true;

  @Override
  public void publish(String topic, Object event) {
    if (!connected) {
      throw new RuntimeException("Event port is not connected");
    }

    log.debug("Publishing event to topic '{}': {}", topic, event.getClass().getSimpleName());

    // Deliver to generic subscribers
    List<Consumer<Object>> subscribers = topicSubscribers.get(topic);
    if (subscribers != null) {
      for (Consumer<Object> subscriber : subscribers) {
        try {
          subscriber.accept(event);
          log.trace("Delivered event to generic subscriber for topic '{}'", topic);
        } catch (Exception e) {
          log.error("Error delivering event to subscriber for topic '{}'", topic, e);
        }
      }
    }

    // Deliver to typed subscribers
    Map<Class<?>, List<Consumer<Object>>> typedSubs = typedSubscribers.get(topic);
    if (typedSubs != null) {
      List<Consumer<Object>> typeSubscribers = typedSubs.get(event.getClass());
      if (typeSubscribers != null) {
        for (Consumer<Object> subscriber : typeSubscribers) {
          try {
            subscriber.accept(event);
            log.trace("Delivered event to typed subscriber for topic '{}'", topic);
          } catch (Exception e) {
            log.error("Error delivering event to typed subscriber for topic '{}'", topic, e);
          }
        }
      }
    }

    log.debug(
        "Published event to topic '{}' - delivered to {} subscribers",
        topic,
        getTotalSubscriberCount(topic));
  }

  @Override
  public void publishAsync(String topic, Object event) {
    // In this simple implementation, async is the same as sync
    // In a real implementation, this would use a thread pool
    publish(topic, event);
  }

  @Override
  public String subscribe(String topic, Consumer<Object> handler) {
    log.debug("Subscribing to topic '{}'", topic);

    String subscriptionId = "sub-" + subscriptionCounter.getAndIncrement();

    topicSubscribers.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(handler);

    subscriptionToTopic.put(subscriptionId, topic);
    subscriptionToHandler.put(subscriptionId, handler);

    log.info(
        "Subscribed to topic '{}' with ID {} - total subscribers: {}",
        topic,
        subscriptionId,
        topicSubscribers.get(topic).size());

    return subscriptionId;
  }

  @Override
  public String subscribe(String topic, Class<?> eventType, Consumer<Object> handler) {
    log.debug("Subscribing to topic '{}' for event type '{}'", topic, eventType.getSimpleName());

    String subscriptionId = "sub-" + subscriptionCounter.getAndIncrement();

    typedSubscribers
        .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(eventType, k -> new CopyOnWriteArrayList<>())
        .add(handler);

    subscriptionToTopic.put(subscriptionId, topic);
    subscriptionToHandler.put(subscriptionId, handler);

    log.info(
        "Subscribed to topic '{}' for type '{}' with ID {} - total typed subscribers: {}",
        topic,
        eventType.getSimpleName(),
        subscriptionId,
        typedSubscribers.get(topic).get(eventType).size());

    return subscriptionId;
  }

  @Override
  public int unsubscribeMatching(Predicate<String> predicate) {
    int removed = 0;

    Iterator<Map.Entry<String, String>> iterator = subscriptionToTopic.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      String subscriptionId = entry.getKey();

      if (predicate.test(subscriptionId)) {
        String topic = entry.getValue();
        Consumer<Object> handler = subscriptionToHandler.get(subscriptionId);

        // Remove from topic subscribers
        List<Consumer<Object>> subscribers = topicSubscribers.get(topic);
        if (subscribers != null) {
          subscribers.remove(handler);
        }

        // Remove from typed subscribers
        Map<Class<?>, List<Consumer<Object>>> typedSubs = typedSubscribers.get(topic);
        if (typedSubs != null) {
          typedSubs.values().forEach(list -> list.remove(handler));
        }

        // Remove from tracking maps
        iterator.remove();
        subscriptionToHandler.remove(subscriptionId);
        removed++;
      }
    }

    log.info("Unsubscribed {} subscriptions matching predicate", removed);
    return removed;
  }

  @Override
  public boolean unsubscribe(String subscriptionId) {
    log.debug("Unsubscribing subscription '{}'", subscriptionId);

    String topic = subscriptionToTopic.get(subscriptionId);
    if (topic == null) {
      log.debug("Subscription '{}' not found", subscriptionId);
      return false;
    }

    Consumer<Object> handler = subscriptionToHandler.get(subscriptionId);
    if (handler == null) {
      log.debug("Handler for subscription '{}' not found", subscriptionId);
      return false;
    }

    // Remove from topic subscribers
    List<Consumer<Object>> subscribers = topicSubscribers.get(topic);
    if (subscribers != null) {
      subscribers.remove(handler);
    }

    // Remove from typed subscribers
    Map<Class<?>, List<Consumer<Object>>> typedSubs = typedSubscribers.get(topic);
    if (typedSubs != null) {
      typedSubs.values().forEach(list -> list.remove(handler));
    }

    // Remove from tracking maps
    subscriptionToTopic.remove(subscriptionId);
    subscriptionToHandler.remove(subscriptionId);

    log.info("Unsubscribed subscription '{}' from topic '{}'", subscriptionId, topic);
    return true;
  }

  @Override
  public String getSubscriptionTopic(String subscriptionId) {
    return subscriptionToTopic.get(subscriptionId);
  }

  @Override
  public boolean isConnected() {
    return connected;
  }

  @Override
  public void close() {
    log.info("Closing event port");
    connected = false;
    topicSubscribers.clear();
    typedSubscribers.clear();
    log.info("Event port closed");
  }

  /** Reconnects the event port. Useful for testing connection handling. */
  public void reconnect() {
    log.info("Reconnecting event port");
    connected = true;
  }

  /** Gets the total number of subscribers for a topic. */
  private int getTotalSubscriberCount(String topic) {
    int genericCount = topicSubscribers.getOrDefault(topic, List.of()).size();
    int typedCount =
        typedSubscribers.getOrDefault(topic, Map.of()).values().stream().mapToInt(List::size).sum();
    return genericCount + typedCount;
  }

  /** Gets all subscribed topics for debugging. */
  public Set<String> getSubscribedTopics() {
    Set<String> topics = new HashSet<>(topicSubscribers.keySet());
    topics.addAll(typedSubscribers.keySet());
    return topics;
  }

  /** Gets subscription statistics for monitoring. */
  public Map<String, Integer> getSubscriptionStats() {
    Map<String, Integer> stats = new HashMap<>();

    // Add generic subscribers
    topicSubscribers.forEach(
        (topic, subscribers) -> stats.put(topic + " (generic)", subscribers.size()));

    // Add typed subscribers
    typedSubscribers.forEach(
        (topic, typeMap) ->
            typeMap.forEach(
                (type, subscribers) ->
                    stats.put(topic + " (" + type.getSimpleName() + ")", subscribers.size())));

    return stats;
  }

  /** Clears all subscriptions. Useful for testing. */
  public void clear() {
    log.info("Clearing all subscriptions");
    topicSubscribers.clear();
    typedSubscribers.clear();
    subscriptionToTopic.clear();
    subscriptionToHandler.clear();
  }
}
