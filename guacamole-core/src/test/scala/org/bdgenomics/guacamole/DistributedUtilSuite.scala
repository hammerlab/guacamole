/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bdgenomics.guacamole

import org.scalatest.matchers.ShouldMatchers
import com.google.common.collect._
import org.scalatest.matchers.ShouldMatchers._

class DistributedUtilSuite extends TestUtil.SparkFunSuite with ShouldMatchers {

  test("partitionLociUniformly") {
    val set = LociSet.parse("chr21:100-200,chr20:0-10,chr20:8-15,chr20:100-121,empty:10-10")
    val result1 = DistributedUtil.partitionLociUniformly(1, set).asInverseMap
    result1(0) should equal(set)

    val result2 = DistributedUtil.partitionLociUniformly(2, set).asInverseMap
    result2(0).count should equal(set.count / 2)
    result2(1).count should equal(set.count / 2)
    result2(0).union(result2(1)) should equal(set)
  }

}