package com.streamfirst.iceberg.hybrid.domain

import java.time.Instant

/** Represents the progress of a sync operation */
case class SyncProgress(
  totalEvents: Int,
  processedEvents: Int,
  successfulEvents: Int,
  failedEvents: Int,
  startTime: Instant,
  lastUpdateTime: Instant,
  estimatedCompletionTime: Option[Instant] = None
):
  def percentComplete: Double = 
    if totalEvents == 0 then 100.0
    else (processedEvents.toDouble / totalEvents.toDouble) * 100.0

  def withEventProcessed(success: Boolean): SyncProgress =
    val newProcessed = processedEvents + 1
    val newSuccessful = if success then successfulEvents + 1 else successfulEvents
    val newFailed = if success then failedEvents else failedEvents + 1
    val now = Instant.now()
    
    val estimatedCompletion = if newProcessed > 0 && newProcessed < totalEvents then
      val elapsedMillis = now.toEpochMilli - startTime.toEpochMilli
      val avgTimePerEvent = elapsedMillis.toDouble / newProcessed.toDouble
      val remainingEvents = totalEvents - newProcessed
      val remainingMillis = (remainingEvents * avgTimePerEvent).toLong
      Some(now.plusMillis(remainingMillis))
    else None
    
    copy(
      processedEvents = newProcessed,
      successfulEvents = newSuccessful,
      failedEvents = newFailed,
      lastUpdateTime = now,
      estimatedCompletionTime = estimatedCompletion
    )

object SyncProgress:
  def start(totalEvents: Int): SyncProgress =
    val now = Instant.now()
    SyncProgress(
      totalEvents = totalEvents,
      processedEvents = 0,
      successfulEvents = 0,
      failedEvents = 0,
      startTime = now,
      lastUpdateTime = now
    )