package com.streamfirst.iceberg.hybrid.adapters;

import com.streamfirst.iceberg.hybrid.ports.EventPort;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * In-memory implementation of EventPort for testing and development.
 * Provides simple publish/subscribe functionality using in-memory collections.
 * Events are delivered synchronously within the same JVM.
 */
@Slf4j
public class InMemoryEventAdapter implements EventPort {
    
    private final Map<String, List<Consumer<Object>>> topicSubscribers = new ConcurrentHashMap<>();
    private final Map<String, Map<Class<?>, List<Consumer<Object>>>> typedSubscribers = new ConcurrentHashMap<>();
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
        
        log.debug("Published event to topic '{}' - delivered to {} subscribers", 
                 topic, getTotalSubscriberCount(topic));
    }

    @Override
    public void publishAsync(String topic, Object event) {
        // In this simple implementation, async is the same as sync
        // In a real implementation, this would use a thread pool
        publish(topic, event);
    }

    @Override
    public void subscribe(String topic, Consumer<Object> handler) {
        log.debug("Subscribing to topic '{}'", topic);
        
        topicSubscribers.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>())
                       .add(handler);
        
        log.info("Subscribed to topic '{}' - total subscribers: {}", 
                topic, topicSubscribers.get(topic).size());
    }

    @Override
    public void subscribe(String topic, Class<?> eventType, Consumer<Object> handler) {
        log.debug("Subscribing to topic '{}' for event type '{}'", topic, eventType.getSimpleName());
        
        typedSubscribers.computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                       .computeIfAbsent(eventType, k -> new CopyOnWriteArrayList<>())
                       .add(handler);
        
        log.info("Subscribed to topic '{}' for type '{}' - total typed subscribers: {}", 
                topic, eventType.getSimpleName(), 
                typedSubscribers.get(topic).get(eventType).size());
    }

    @Override
    public void unsubscribe(String topic) {
        log.debug("Unsubscribing from topic '{}'", topic);
        
        List<Consumer<Object>> removed = topicSubscribers.remove(topic);
        Map<Class<?>, List<Consumer<Object>>> typedRemoved = typedSubscribers.remove(topic);
        
        int removedCount = (removed != null ? removed.size() : 0) + 
                          (typedRemoved != null ? typedRemoved.values().stream()
                                                               .mapToInt(List::size)
                                                               .sum() : 0);
        
        log.info("Unsubscribed from topic '{}' - removed {} subscribers", topic, removedCount);
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

    /**
     * Reconnects the event port. Useful for testing connection handling.
     */
    public void reconnect() {
        log.info("Reconnecting event port");
        connected = true;
    }

    /**
     * Gets the total number of subscribers for a topic.
     */
    private int getTotalSubscriberCount(String topic) {
        int genericCount = topicSubscribers.getOrDefault(topic, List.of()).size();
        int typedCount = typedSubscribers.getOrDefault(topic, Map.of()).values().stream()
                                        .mapToInt(List::size)
                                        .sum();
        return genericCount + typedCount;
    }

    /**
     * Gets all subscribed topics for debugging.
     */
    public Set<String> getSubscribedTopics() {
        Set<String> topics = new HashSet<>(topicSubscribers.keySet());
        topics.addAll(typedSubscribers.keySet());
        return topics;
    }

    /**
     * Gets subscription statistics for monitoring.
     */
    public Map<String, Integer> getSubscriptionStats() {
        Map<String, Integer> stats = new HashMap<>();
        
        // Add generic subscribers
        topicSubscribers.forEach((topic, subscribers) -> 
            stats.put(topic + " (generic)", subscribers.size()));
        
        // Add typed subscribers
        typedSubscribers.forEach((topic, typeMap) -> 
            typeMap.forEach((type, subscribers) -> 
                stats.put(topic + " (" + type.getSimpleName() + ")", subscribers.size())));
        
        return stats;
    }

    /**
     * Clears all subscriptions. Useful for testing.
     */
    public void clear() {
        log.info("Clearing all subscriptions");
        topicSubscribers.clear();
        typedSubscribers.clear();
    }
}