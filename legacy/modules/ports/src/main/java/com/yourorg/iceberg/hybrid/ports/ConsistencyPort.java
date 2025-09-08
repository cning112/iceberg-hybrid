package com.yourorg.iceberg.hybrid.ports;

import com.yourorg.iceberg.hybrid.domain.*;
import java.util.Optional;

public interface ConsistencyPort {
  Optional<ConsistencyToken> loadToken(TableId tableId);
  void saveToken(TableId tableId, ConsistencyToken token);
}
