package com.streamfirst.iceberg.hybrid.domain

import zio.*
import zio.test.*
import zio.test.Assertion.*

object RegionSpec extends ZIOSpecDefault:
  def spec = suite("Region")(
    test("should create a valid region with id and displayName") {
      val region = Region("us-east-1", "US East")
      assertTrue(
        region.id == "us-east-1",
        region.displayName == "US East"
      )
    },
    
    test("should fail to create region with empty id") {
      val result = ZIO.attempt(Region("", "Some Display Name"))
      assertZIO(result.exit)(fails(isSubtype[IllegalArgumentException](anything)))
    },
    
    test("should fail to create region with empty displayName") {
      val result = ZIO.attempt(Region("some-id", ""))
      assertZIO(result.exit)(fails(isSubtype[IllegalArgumentException](anything)))
    },
    
    test("predefined regions should have correct values") {
      assertTrue(
        Region.UsEast1.id == "us-east-1",
        Region.UsEast1.displayName == "US East (N. Virginia)",
        Region.EuWest1.id == "eu-west-1", 
        Region.EuWest1.displayName == "Europe (Ireland)",
        Region.ApNortheast1.id == "ap-northeast-1",
        Region.ApNortheast1.displayName == "Asia Pacific (Tokyo)"
      )
    }
  )