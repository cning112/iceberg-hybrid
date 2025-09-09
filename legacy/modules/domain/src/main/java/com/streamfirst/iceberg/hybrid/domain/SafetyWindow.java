package com.streamfirst.iceberg.hybrid.domain;

/**
 * Configuration for safety delays in garbage collection coordination.
 * 
 * <p>Defines different grace periods for on-premise and cloud environments to ensure
 * that files are not deleted while they might still be referenced by active queries
 * or pending replication operations.
 * 
 * <p>The cloud delay is typically longer than the on-premise delay to account for:
 * <ul>
 * <li>Network latency in cross-environment coordination</li>
 * <li>Longer-running analytical queries in cloud environments</li>
 * <li>Additional safety margin for critical cloud-based workloads</li>
 * </ul>
 * 
 * <p>These delays are measured from the {@link DeletePlan#generatedAt()} timestamp.
 * 
 * @param onPremDelaySeconds minimum seconds to wait before allowing on-premise deletion
 * @param cloudDelaySeconds minimum seconds to wait before allowing cloud deletion
 */
public record SafetyWindow(long onPremDelaySeconds, long cloudDelaySeconds) {}
