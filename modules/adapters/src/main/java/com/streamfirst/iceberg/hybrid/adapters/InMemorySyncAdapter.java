package com.streamfirst.iceberg.hybrid.adapters;

import com.streamfirst.iceberg.hybrid.domain.*;
import com.streamfirst.iceberg.hybrid.ports.SyncPort;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * In-memory implementation of SyncPort for testing and development.
 * Provides simple event queuing and status tracking using in-memory collections.
 * Data is lost when the application stops - not suitable for production use.
 */
@Slf4j
public class InMemorySyncAdapter implements SyncPort {
    
    private final Map<String, SyncEvent> events = new ConcurrentHashMap<>();
    private final Map<Region, List<String>> pendingEventsByRegion = new ConcurrentHashMap<>();
    private final Map<TableId, List<String>> eventsByTable = new ConcurrentHashMap<>();
    private final AtomicLong eventCounter = new AtomicLong(1);

    @Override
    public void publishSyncEvent(SyncEvent event) {
        log.debug("Publishing sync event: {}", event);
        
        // Store the event
        events.put(event.getEventId(), event);
        
        // Add to pending queue for target region
        pendingEventsByRegion.computeIfAbsent(event.getTargetRegion(), k -> new ArrayList<>())
                            .add(event.getEventId());
        
        // Add to table history
        eventsByTable.computeIfAbsent(event.getTableId(), k -> new ArrayList<>())
                    .add(event.getEventId());
        
        log.info("Published sync event {} for table {} to region {}", 
                 event.getEventId(), event.getTableId(), event.getTargetRegion());
    }

    @Override
    public List<SyncEvent> getPendingEvents(Region targetRegion) {
        List<String> eventIds = pendingEventsByRegion.get(targetRegion);
        if (eventIds == null) {
            log.debug("No pending events found for region {}", targetRegion);
            return List.of();
        }
        
        List<SyncEvent> pendingEvents = eventIds.stream()
                .map(events::get)
                .filter(Objects::nonNull)
                .filter(event -> event.getStatus() == SyncEvent.Status.PENDING)
                .sorted(Comparator.comparing(SyncEvent::getCreatedAt))
                .toList();
        
        log.debug("Found {} pending events for region {}", pendingEvents.size(), targetRegion);
        return pendingEvents;
    }

    @Override
    public void updateEventStatus(String eventId, SyncEvent.Status status) {
        SyncEvent event = events.get(eventId);
        if (event == null) {
            throw new IllegalArgumentException("Event not found: " + eventId);
        }
        
        log.debug("Updating event {} status from {} to {}", 
                 eventId, event.getStatus(), status);
        
        SyncEvent updatedEvent = event.withStatus(status, Instant.now());
        events.put(eventId, updatedEvent);
        
        // Remove from pending queue if completed or failed
        if (status == SyncEvent.Status.COMPLETED || status == SyncEvent.Status.FAILED) {
            List<String> pendingIds = pendingEventsByRegion.get(event.getTargetRegion());
            if (pendingIds != null) {
                pendingIds.remove(eventId);
            }
        }
        
        log.debug("Updated event {} status to {}", eventId, status);
    }

    @Override
    public SyncEvent createMetadataSyncEvent(TableMetadata metadata, Region targetRegion) {
        String eventId = "metadata-sync-" + eventCounter.getAndIncrement();
        Instant now = Instant.now();
        
        SyncEvent event = new SyncEvent(
            eventId,
            SyncEvent.Type.METADATA_SYNC,
            metadata.getTableId(),
            metadata.getCommitId(),
            metadata.getSourceRegion(),
            targetRegion,
            SyncEvent.Status.PENDING,
            now,
            now
        );
        
        log.debug("Created metadata sync event {} for table {} from {} to {}", 
                 eventId, metadata.getTableId(), metadata.getSourceRegion(), targetRegion);
        return event;
    }

    @Override
    public SyncEvent createDataSyncEvent(TableMetadata metadata, List<String> dataFiles, Region targetRegion) {
        String eventId = "data-sync-" + eventCounter.getAndIncrement();
        Instant now = Instant.now();
        
        SyncEvent event = new SyncEvent(
            eventId,
            SyncEvent.Type.DATA_SYNC,
            metadata.getTableId(),
            metadata.getCommitId(),
            metadata.getSourceRegion(),
            targetRegion,
            SyncEvent.Status.PENDING,
            now,
            now
        );
        
        log.debug("Created data sync event {} for table {} with {} files from {} to {}", 
                 eventId, metadata.getTableId(), dataFiles.size(), 
                 metadata.getSourceRegion(), targetRegion);
        return event;
    }

    @Override
    public List<SyncEvent> getEventHistory(TableId tableId, Region region) {
        List<String> eventIds = eventsByTable.get(tableId);
        if (eventIds == null) {
            log.debug("No events found for table {}", tableId);
            return List.of();
        }
        
        List<SyncEvent> tableEvents = eventIds.stream()
                .map(events::get)
                .filter(Objects::nonNull)
                .filter(event -> event.getTargetRegion().equals(region))
                .sorted(Comparator.comparing(SyncEvent::getCreatedAt))
                .toList();
        
        log.debug("Found {} events for table {} in region {}", 
                 tableEvents.size(), tableId, region);
        return tableEvents;
    }

    @Override
    public List<SyncEvent> getFailedEvents(Region region) {
        List<String> eventIds = pendingEventsByRegion.get(region);
        if (eventIds == null) {
            log.debug("No events found for region {}", region);
            return List.of();
        }
        
        List<SyncEvent> failedEvents = eventIds.stream()
                .map(events::get)
                .filter(Objects::nonNull)
                .filter(event -> event.getStatus() == SyncEvent.Status.FAILED)
                .sorted(Comparator.comparing(SyncEvent::getUpdatedAt).reversed())
                .toList();
        
        log.debug("Found {} failed events for region {}", failedEvents.size(), region);
        return failedEvents;
    }

    @Override
    public void retryFailedEvent(String eventId) {
        SyncEvent event = events.get(eventId);
        if (event == null) {
            throw new IllegalArgumentException("Event not found: " + eventId);
        }
        
        if (event.getStatus() != SyncEvent.Status.FAILED) {
            throw new IllegalArgumentException("Event " + eventId + " is not in FAILED status");
        }
        
        log.info("Retrying failed event {}", eventId);
        
        // Reset status to PENDING
        SyncEvent retriedEvent = event.withStatus(SyncEvent.Status.PENDING, Instant.now());
        events.put(eventId, retriedEvent);
        
        // Add back to pending queue if not already there
        List<String> pendingIds = pendingEventsByRegion.computeIfAbsent(
            event.getTargetRegion(), k -> new ArrayList<>());
        if (!pendingIds.contains(eventId)) {
            pendingIds.add(eventId);
        }
        
        log.info("Retried event {} - reset to PENDING status", eventId);
    }

    /**
     * Clears all sync data. Useful for testing.
     */
    public void clear() {
        log.info("Clearing all sync data");
        events.clear();
        pendingEventsByRegion.clear();
        eventsByTable.clear();
        eventCounter.set(1);
    }

    /**
     * Gets the total number of events across all states.
     */
    public int getTotalEventCount() {
        return events.size();
    }

    /**
     * Gets event count by status for monitoring.
     */
    public Map<SyncEvent.Status, Long> getEventCountByStatus() {
        return events.values().stream()
                .collect(Collectors.groupingBy(
                    SyncEvent::getStatus, 
                    Collectors.counting()));
    }

    /**
     * Gets event count by type for monitoring.
     */
    public Map<SyncEvent.Type, Long> getEventCountByType() {
        return events.values().stream()
                .collect(Collectors.groupingBy(
                    SyncEvent::getType, 
                    Collectors.counting()));
    }

    /**
     * Gets all events for debugging purposes.
     */
    public Collection<SyncEvent> getAllEvents() {
        return new ArrayList<>(events.values());
    }
}