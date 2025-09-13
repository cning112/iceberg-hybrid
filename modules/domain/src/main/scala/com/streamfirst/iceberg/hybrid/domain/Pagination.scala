package com.streamfirst.iceberg.hybrid.domain

/** Represents pagination parameters for listing operations */
case class PaginationRequest(
  pageSize: Int,
  continuationToken: Option[String] = None
):
  require(pageSize > 0, "Page size must be positive")
  require(pageSize <= 10000, "Page size cannot exceed 10000")

/** Represents a paginated response */
case class PaginatedResult[T](
  items: List[T],
  continuationToken: Option[String] = None,
  hasMore: Boolean = false
):
  def map[U](f: T => U): PaginatedResult[U] = 
    PaginatedResult(items.map(f), continuationToken, hasMore)

object PaginationRequest:
  val default: PaginationRequest = PaginationRequest(pageSize = 1000)
  
  def withSize(size: Int): PaginationRequest = PaginationRequest(pageSize = size)
  
  def withToken(size: Int, token: String): PaginationRequest = 
    PaginationRequest(pageSize = size, continuationToken = Some(token))