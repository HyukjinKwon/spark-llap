/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.sql.hive.llap

import org.scalatest.FunSuite

class TestJavaProxy extends FunSuite {
  test("HiveWarehouseBuilder") {
    val builderTest = new HiveWarehouseBuilderTest()
    try {
      builderTest.setUp()
      builderTest.testAllBuilderConfig()
    } finally {
      builderTest.tearDown()
    }
  }

  test("HiveWarehouseSession") {
    val hiveWarehouseSessionTest = new HiveWarehouseSessionHiveQlTest()

    def withSetUpAndTearDown(test: () => Unit): Unit = try {
      hiveWarehouseSessionTest.setUp()
      test()
    } finally {
      hiveWarehouseSessionTest.tearDown()
    }

    withSetUpAndTearDown(hiveWarehouseSessionTest.testCreateDatabase)
    withSetUpAndTearDown(hiveWarehouseSessionTest.testCreateTable)
    withSetUpAndTearDown(hiveWarehouseSessionTest.testExecuteQuery)
    withSetUpAndTearDown(hiveWarehouseSessionTest.testSetDatabase)
    withSetUpAndTearDown(hiveWarehouseSessionTest.testShowTable)
  }
}
