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

package org.hammerlab.guacamole.variants

import com.esotericsoftware.kryo.Kryo
import org.hammerlab.guacamole.util.{Bases, GuacFunSuite, KryoTestRegistrar}
import org.scalatest.Matchers

class AlleleSuiteRegistrar extends KryoTestRegistrar {
  override def registerTestClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Allele], new AlleleSerializer)
  }
}

class AlleleSuite extends GuacFunSuite with Matchers {

  override def registrar = "org.hammerlab.guacamole.variants.AlleleSuiteRegistrar"

  test("isVariant") {
    val mismatch = Allele(Seq(Bases.T), Seq(Bases.A))
    mismatch.isVariant should be(true)

    val reference = Allele(Seq(Bases.T), Seq(Bases.T))
    reference.isVariant should be(false)

    val deletion = Allele(Seq(Bases.T, Bases.T, Bases.T), Seq(Bases.T))
    deletion.isVariant should be(true)

    val insertion = Allele(Seq(Bases.T), Seq(Bases.T, Bases.A, Bases.A))
    insertion.isVariant should be(true)

  }

  test("serializing allele") {
    val a1 = Allele(Seq(Bases.T), Seq(Bases.A))

    val a1Serialized = serialize(a1)
    val a1Deserialized = deserialize[Allele](a1Serialized)

    assert(a1 === a1Deserialized)

    val a2 = Allele(Seq(Bases.T, Bases.T, Bases.C), Seq(Bases.A))
    val a2Serialized = serialize(a2)
    val a2Deserialized = deserialize[Allele](a2Serialized)

    assert(a2 === a2Deserialized)
  }

}
