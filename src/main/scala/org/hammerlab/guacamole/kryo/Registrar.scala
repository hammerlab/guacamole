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

package org.hammerlab.guacamole.kryo

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.bdgenomics.adam.models.{SequenceDictionary, SequenceRecord}
import org.bdgenomics.adam.serialization.ADAMKryoRegistrator
import org.hammerlab.guacamole.commands.jointcaller.kryo.{Registrar => JointCallerRegistrar}
import org.hammerlab.guacamole.distributed.LociPartitionUtils.{LociPartitioning, MicroPartitionIdx, PartitionIdx}
import org.hammerlab.guacamole.distributed.TaskPosition
import org.hammerlab.guacamole.loci.map.{LociMap, Contig => LociMapContig, ContigSerializer => LociMapContigSerializer, Serializer => LociMapSerializer}
import org.hammerlab.guacamole.loci.set.{LociSet, Contig => LociSetContig, ContigSerializer => LociSetContigSerializer, Serializer => LociSetSerializer}
import org.hammerlab.guacamole.reads.{MappedRead, MappedReadSerializer, MateAlignmentProperties, PairedRead, Read, UnmappedRead, UnmappedReadSerializer}
import org.hammerlab.guacamole.variants.{Allele, AlleleEvidence, AlleleSerializer, CalledAllele}

class Registrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {

    // Register ADAM serializers.
    new ADAMKryoRegistrator().registerClasses(kryo)

    // Register Joint-Caller serializers.
    new JointCallerRegistrar().registerClasses(kryo)

    // SequenceDictionary (and its records) are serialized when loading Reads from ADAM, in
    // Read.ADAMSequenceDictionaryRDDAggregator. ADAM should register these itself.
    kryo.register(classOf[SequenceDictionary])
    kryo.register(classOf[SequenceRecord])

    // Reads
    kryo.register(classOf[MappedRead], new MappedReadSerializer)
    kryo.register(classOf[Array[MappedRead]])
    kryo.register(classOf[MateAlignmentProperties])
    kryo.register(classOf[Array[Read]])
    kryo.register(classOf[UnmappedRead], new UnmappedReadSerializer)

    kryo.register(classOf[PairedRead[_]])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofByte])  // PairedRead

    // LociSet is serialized when broadcast in InputFilters.filterRDD. Serde'ing a LociSet delegates to an Array of
    // Contigs.
    kryo.register(classOf[LociSet], new LociSetSerializer)
    kryo.register(classOf[Array[LociSet]])
    kryo.register(classOf[LociSetContig], new LociSetContigSerializer)
    kryo.register(classOf[Array[LociSetContig]])

    // LociMap is serialized when broadcast in LociPartitionUtils.partitionLociByApproximateDepth.
    kryo.register(classOf[LociPartitioning], new LociMapSerializer[PartitionIdx])
    kryo.register(classOf[LociMapContig[PartitionIdx]], new LociMapContigSerializer[PartitionIdx])
    kryo.register(classOf[LociMapContig[MicroPartitionIdx]], new LociMapContigSerializer[MicroPartitionIdx])

    // Serialized in WindowFlatMapUtils.
    kryo.register(classOf[TaskPosition])

    // Germline-assembly caller flatmaps some CalledAlleles.
    kryo.register(classOf[Array[CalledAllele]])
    kryo.register(classOf[CalledAllele])
    kryo.register(classOf[Allele], new AlleleSerializer)
    kryo.register(classOf[AlleleEvidence])

    // Seems to be used when collecting RDDs of Objects of various kinds.
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])

    // https://mail-archives.apache.org/mod_mbox/spark-user/201504.mbox/%3CCAC95X6JgXQ3neXF6otj6a+F_MwJ9jbj9P-Ssw3Oqkf518_eT1w@mail.gmail.com%3E
    kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1"))
    kryo.register(classOf[java.lang.Class[_]])
  }
}
