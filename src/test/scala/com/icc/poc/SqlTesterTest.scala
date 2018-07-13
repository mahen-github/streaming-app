package com.icc.poc

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.SharedSparkContext

class SqlTesterTest extends FunSuite with SharedSparkContext {

  override def beforeAll() {
    super.beforeAll()
  }

  override def afterAll() {
    super.afterAll()
  }

  test("Testting the process") {
    val path = getClass.getResource("/sample_cc.csv").getPath
    val sqlTester = new SqlTester(sc)
    sqlTester.process(path)
  }
}