package com.streamfirst.iceberg.hybrid.ports;

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Port for asynchronous event messaging infrastructure.
 * Provides publish/subscribe capabilities for inter-service communication.
 */
public interface EventPort {
    
    /**
     * Publishes an event synchronously to a topic.
     * Blocks until the event is delivered to the message broker.
     * 
     * @param topic the topic to publish to
     * @param event the event object to publish
     * @throws RuntimeException if publishing fails
     */
    void publish(String topic, Object event);
    
    /**
     * Publishes an event asynchronously to a topic.
     * Returns immediately without waiting for delivery confirmation.
     * 
     * @param topic the topic to publish to
     * @param event the event object to publish
     */
    void publishAsync(String topic, Object event);
    
    /**
     * Subscribes to a topic with a handler for all event types.
     * 
     * @param topic the topic to subscribe to
     * @param handler the handler to process received events
     * @return subscription ID for managing this specific subscription
     * @throws RuntimeException if subscription fails
     */
    String subscribe(String topic, Consumer<Object> handler);
    
    /**
     * Subscribes to a topic with type-safe event handling.
     * Only events of the specified type will be passed to the handler.
     * 
     * @param topic the topic to subscribe to
     * @param eventType the expected event type
     * @param handler the handler to process received events
     * @return subscription ID for managing this specific subscription
     * @throws RuntimeException if subscription fails
     */
    String subscribe(String topic, Class<?> eventType, Consumer<Object> handler);
    
    /**
     * Unsubscribes subscriptions matching the specified criteria.
     * Provides flexible subscription management for complex event scenarios.
     * 
     * @param predicate the filter criteria for subscriptions to remove
     * @return number of subscriptions that were removed
     */
    int unsubscribeMatching(Predicate<String> predicate);
    
    /**
     * Unsubscribes a specific subscription by its ID.
     * 
     * @param subscriptionId the subscription ID to remove
     * @return true if subscription was found and removed, false otherwise
     */
    boolean unsubscribe(String subscriptionId);
    
    /**
     * Unsubscribes all subscriptions from a topic.
     * 
     * @param topic the topic to unsubscribe from
     * @return number of subscriptions that were removed
     */
    default int unsubscribeFromTopic(String topic) {
        return unsubscribeMatching(subscriptionId -> 
            getSubscriptionTopic(subscriptionId).equals(topic));
    }
    
    /**
     * Gets the topic for a given subscription ID.
     * 
     * @param subscriptionId the subscription ID
     * @return the topic this subscription is bound to
     */
    String getSubscriptionTopic(String subscriptionId);
    
    /**
     * Checks if the event port is connected to the message broker.
     * 
     * @return true if connected, false otherwise
     */
    boolean isConnected();
    
    /**
     * Closes the event port and releases resources.
     * Unsubscribes from all topics and closes connections.
     */
    void close();
}