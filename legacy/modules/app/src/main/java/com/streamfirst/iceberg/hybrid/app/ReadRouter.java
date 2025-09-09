package com.streamfirst.iceberg.hybrid.app;

import com.streamfirst.iceberg.hybrid.domain.*;
import com.streamfirst.iceberg.hybrid.ports.ConsistencyPort;

import java.util.Optional;

public final class ReadRouter {

  public enum RoutingPolicy { PREFER_CLOUD, PREFER_ONPREM, MEET_WATERMARK }

  private final ConsistencyPort consistency;

  public ReadRouter(ConsistencyPort consistency) {
    this.consistency = consistency;
  }

  public Route route(TableId tableId, SnapshotId requested, RoutingPolicy policy) {
    Optional<ConsistencyToken> token = consistency.loadToken(tableId);

    boolean cloudOk = token.isPresent() &&
        (requested.commitTs().equals(token.get().highWatermarkTs())
         || requested.commitTs().isBefore(token.get().highWatermarkTs()));

    return switch (policy) {
      case PREFER_CLOUD -> new Route(cloudOk ? Target.CLOUD : Target.ONPREM, requested);
      case PREFER_ONPREM -> new Route(Target.ONPREM, requested);
      case MEET_WATERMARK -> new Route(cloudOk ? Target.CLOUD : Target.ONPREM, requested);
    };
  }

  public enum Target { CLOUD, ONPREM }
  public record Route(Target target, SnapshotId snapshot) {}
}
