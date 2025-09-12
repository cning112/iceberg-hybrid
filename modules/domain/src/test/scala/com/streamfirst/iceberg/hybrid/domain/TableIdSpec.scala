package com.streamfirst.iceberg.hybrid.domain

import zio.*
import zio.test.*
import zio.test.Assertion.*

object TableIdSpec extends ZIOSpecDefault:
  def spec = suite("TableId")(
    test("should create a valid table id with namespace and name") {
      val tableId = TableId("analytics", "user_events")
      assertTrue(
        tableId.namespace == "analytics",
        tableId.name == "user_events",
        tableId.fullyQualifiedName == "analytics.user_events"
      )
    },
    
    test("should fail to create table id with empty namespace") {
      val result = ZIO.attempt(TableId("", "some_table"))
      assertZIO(result.exit)(fails(isSubtype[IllegalArgumentException](anything)))
    },
    
    test("should fail to create table id with empty name") {
      val result = ZIO.attempt(TableId("some_namespace", ""))
      assertZIO(result.exit)(fails(isSubtype[IllegalArgumentException](anything)))
    },
    
    test("should convert to string via given conversion") {
      val tableId = TableId("warehouse", "transactions")
      val str: String = tableId
      assertTrue(str == "warehouse.transactions")
    },
    
    test("should handle nested namespace correctly") {
      val tableId = TableId("warehouse.sales", "daily_reports")
      assertTrue(
        tableId.namespace == "warehouse.sales",
        tableId.name == "daily_reports",
        tableId.fullyQualifiedName == "warehouse.sales.daily_reports"
      )
    }
  )