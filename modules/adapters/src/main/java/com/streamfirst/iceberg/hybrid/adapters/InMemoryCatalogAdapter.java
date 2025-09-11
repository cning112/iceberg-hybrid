package com.streamfirst.iceberg.hybrid.adapters;

import com.streamfirst.iceberg.hybrid.domain.*;
import com.streamfirst.iceberg.hybrid.ports.CatalogPort;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;

/**
 * In-memory implementation of CatalogPort for testing and development. Provides a simple,
 * thread-safe catalog that stores all metadata in memory. Data is lost when the application stops -
 * not suitable for production use.
 */
@Slf4j
public class InMemoryCatalogAdapter implements CatalogPort {

  private final Map<TableId, List<TableMetadata>> tableHistory = new ConcurrentHashMap<>();
  private final Map<String, Set<TableId>> namespaces = new ConcurrentHashMap<>();
  private final AtomicLong commitCounter = new AtomicLong(1);

  @Override
  public CommitId commitMetadata(CommitRequest request) {
    log.debug("Committing metadata for table {}", request.getTableId());

    // Generate unique commit ID
    CommitId commitId = new CommitId("commit-" + commitCounter.getAndIncrement());

    // Create new metadata version
    TableMetadata metadata =
        new TableMetadata(
            request.getTableId(),
            commitId,
            request.getSourceRegion(),
            request.getRequestTime(),
            request.getNewDataFiles(),
            request.getUpdatedSchema());

    // Add to table history
    tableHistory.computeIfAbsent(request.getTableId(), k -> new ArrayList<>()).add(metadata);

    // Register namespace
    namespaces
        .computeIfAbsent(request.getTableId().namespace(), k -> new HashSet<>())
        .add(request.getTableId());

    log.info("Committed metadata for table {} with commit ID {}", request.getTableId(), commitId);
    return commitId;
  }

  @Override
  public Optional<TableMetadata> getLatestMetadata(TableId tableId) {
    List<TableMetadata> history = tableHistory.get(tableId);
    if (history == null || history.isEmpty()) {
      log.debug("No metadata found for table {}", tableId);
      return Optional.empty();
    }

    // Return the most recent metadata (last in list)
    TableMetadata latest = history.get(history.size() - 1);
    log.debug("Retrieved latest metadata for table {}: commit {}", tableId, latest.getCommitId());
    return Optional.of(latest);
  }

  @Override
  public Optional<TableMetadata> getMetadata(TableId tableId, CommitId commitId) {
    List<TableMetadata> history = tableHistory.get(tableId);
    if (history == null) {
      log.debug("No metadata found for table {}", tableId);
      return Optional.empty();
    }

    return history.stream()
        .filter(metadata -> metadata.getCommitId().equals(commitId))
        .findFirst()
        .map(
            metadata -> {
              log.debug("Retrieved metadata for table {} commit {}", tableId, commitId);
              return metadata;
            });
  }

  @Override
  public List<TableMetadata> getCommits(TableId tableId, Predicate<TableMetadata> criteria) {
    List<TableMetadata> history = tableHistory.get(tableId);
    if (history == null) {
      log.debug("No metadata found for table {}", tableId);
      return List.of();
    }

    List<TableMetadata> result = history.stream().filter(criteria).toList();

    log.debug("Found {} commits matching criteria for table {}", result.size(), tableId);
    return result;
  }

  @Override
  public List<TableId> listTables(Predicate<TableId> criteria) {
    List<TableId> result =
        namespaces.values().stream().flatMap(Set::stream).filter(criteria).toList();

    log.debug("Found {} tables matching criteria", result.size());
    return result;
  }

  @Override
  public boolean tableExists(TableId tableId) {
    boolean exists = tableHistory.containsKey(tableId);
    log.debug("Table {} exists: {}", tableId, exists);
    return exists;
  }

  @Override
  public void createTable(TableId tableId, String schema, Region sourceRegion) {
    if (tableExists(tableId)) {
      throw new IllegalArgumentException("Table " + tableId + " already exists");
    }

    log.info("Creating table {} in region {}", tableId, sourceRegion);

    // Create initial commit for the table
    CommitId initialCommit = new CommitId("commit-" + commitCounter.getAndIncrement());

    TableMetadata initialMetadata =
        new TableMetadata(
            tableId,
            initialCommit,
            sourceRegion,
            Instant.now(),
            List.of(), // No data files initially
            schema);

    // Add to table history
    tableHistory.put(tableId, new ArrayList<>(List.of(initialMetadata)));

    // Register namespace
    namespaces.computeIfAbsent(tableId.namespace(), k -> new HashSet<>()).add(tableId);

    log.info("Created table {} with initial commit {}", tableId, initialCommit);
  }

  @Override
  public void dropTable(TableId tableId) {
    if (!tableExists(tableId)) {
      throw new IllegalArgumentException("Table " + tableId + " does not exist");
    }

    log.info("Dropping table {}", tableId);

    // Remove from table history
    tableHistory.remove(tableId);

    // Remove from namespace
    Set<TableId> tables = namespaces.get(tableId.namespace());
    if (tables != null) {
      tables.remove(tableId);
      if (tables.isEmpty()) {
        namespaces.remove(tableId.namespace());
      }
    }

    log.info("Dropped table {}", tableId);
  }

  /** Clears all data from the catalog. Useful for testing. */
  public void clear() {
    log.info("Clearing all catalog data");
    tableHistory.clear();
    namespaces.clear();
    commitCounter.set(1);
  }

  /** Gets the total number of tables in the catalog. */
  public int getTableCount() {
    return tableHistory.size();
  }

  /** Gets the total number of commits across all tables. */
  public int getTotalCommitCount() {
    return tableHistory.values().stream().mapToInt(List::size).sum();
  }
}
