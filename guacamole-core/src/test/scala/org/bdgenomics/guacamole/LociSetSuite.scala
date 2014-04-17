/**
 * Copyright 2014. Regents of the University of California.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.guacamole

import org.bdgenomics.adam.avro.{ ADAMContig, ADAMGenotype, ADAMPileup, ADAMRecord, ADAMVariant }
import org.scalatest._
import org.scalatest.matchers.{ ShouldMatchers, Matchers }

class LociSetSuite extends FunSuite with ShouldMatchers {

  test("basic operations on a loci set") {
    LociSet.empty.contigs should have length (0)

    val set = LociSet.parse("chr21:100-200,chr20:0-10,chr20:8-15,chr20:100-120")
    set.contigs should equal(List("chr20", "chr21"))
    set.onContig("chr20").contains(110) should be(true)

    //set.intersects()

  }
}
