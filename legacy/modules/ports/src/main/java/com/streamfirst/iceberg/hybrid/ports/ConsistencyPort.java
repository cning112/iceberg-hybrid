package com.streamfirst.iceberg.hybrid.ports;

import com.streamfirst.iceberg.hybrid.domain.*;
import java.util.Optional;

public interface ConsistencyPort {
  Optional<ConsistencyToken> loadToken(TableId tableId);
  void saveToken(TableId tableId, ConsistencyToken token);
}
