package com.streamfirst.iceberg.hybrid.domain

/** Represents the result of a batch registration operation */
case class BatchRegistrationResult(
  totalRequests: Int,
  successfulRegistrations: Int,
  failedRegistrations: Int,
  failures: List[BatchRegistrationFailure] = Nil
):
  def successRate: Double = 
    if totalRequests == 0 then 100.0
    else (successfulRegistrations.toDouble / totalRequests.toDouble) * 100.0

  def isCompletelySuccessful: Boolean = failedRegistrations == 0

  def withFailure(failure: BatchRegistrationFailure): BatchRegistrationResult =
    copy(
      failedRegistrations = failedRegistrations + 1,
      failures = failure :: failures
    )

/** Represents a single failure in a batch registration */
case class BatchRegistrationFailure(
  tableId: TableId,
  region: Region,
  dataPath: String,
  error: String
)

object BatchRegistrationResult:
  def empty: BatchRegistrationResult = BatchRegistrationResult(0, 0, 0)
  
  def success(count: Int): BatchRegistrationResult = BatchRegistrationResult(count, count, 0)
  
  def fromResults(results: List[Either[BatchRegistrationFailure, Unit]]): BatchRegistrationResult =
    val successful = results.count(_.isRight)
    val failed = results.count(_.isLeft)
    val failures = results.collect { case Left(failure) => failure }
    
    BatchRegistrationResult(
      totalRequests = results.size,
      successfulRegistrations = successful,
      failedRegistrations = failed,
      failures = failures
    )