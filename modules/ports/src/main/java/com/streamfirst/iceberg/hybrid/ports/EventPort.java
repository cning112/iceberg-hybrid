package com.streamfirst.iceberg.hybrid.ports;

import java.util.function.Consumer;

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
     * @throws RuntimeException if subscription fails
     */
    void subscribe(String topic, Consumer<Object> handler);
    
    /**
     * Subscribes to a topic with type-safe event handling.
     * Only events of the specified type will be passed to the handler.
     * 
     * @param topic the topic to subscribe to
     * @param eventType the expected event type
     * @param handler the handler to process received events
     * @throws RuntimeException if subscription fails
     */
    void subscribe(String topic, Class<?> eventType, Consumer<Object> handler);
    
    /**
     * Unsubscribes from a topic.
     * 
     * @param topic the topic to unsubscribe from
     */
    void unsubscribe(String topic);
    
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