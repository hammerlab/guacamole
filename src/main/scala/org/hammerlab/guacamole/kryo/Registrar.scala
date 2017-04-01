package org.hammerlab.guacamole.kryo

import java.nio.file.Path

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.bdgenomics.adam.models.{ SequenceDictionary, SequenceRecord, VariantContext }
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.adam.serialization.ADAMKryoRegistrator
import org.hammerlab.genomics.bases.Base
import org.hammerlab.genomics.loci.map.{ Contig, ContigSerializer, LociMap, Serializer }
import org.hammerlab.genomics.reference.{ ContigLengths, Position }
import org.hammerlab.genomics.{ bases, readsets, reference }
import org.hammerlab.guacamole.jointcaller.kryo.{ Registrar ⇒ JointCallerRegistrar }
import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.loci.partitioning.LociPartitioning
import org.hammerlab.guacamole.loci.partitioning.MicroRegionPartitioner.MicroPartitionIndex
import org.hammerlab.guacamole.variants.{ Allele, AlleleEvidence, CalledAllele, CalledSomaticAllele, Genotype }
import org.hammerlab.magic.accumulables.{ HashMap ⇒ MagicHashMap }
import org.hammerlab.magic.kryo.{ Registrar ⇒ MagicRDDRegistrar }

class Registrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {

    // Register ADAM serializers.
    new ADAMKryoRegistrator().registerClasses(kryo)

    new MagicRDDRegistrar().registerClasses(kryo)

    // Register Joint-Caller serializers.
    new JointCallerRegistrar().registerClasses(kryo)

    // SequenceDictionary (and its records) are serialized when loading Reads from ADAM, in
    // Read.ADAMSequenceDictionaryRDDAggregator. ADAM should register these itself.
    kryo.register(classOf[SequenceDictionary])
    kryo.register(classOf[SequenceRecord])

    kryo.register(classOf[ContigLengths])

    new readsets.Registrar().registerClasses(kryo)
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofByte])  // PairedRead

    // LociMap[Long]s is serialized when broadcast in MicroRegionPartitioner.
    kryo.register(classOf[LociPartitioning])
    kryo.register(classOf[LociMap[MicroPartitionIndex]], new Serializer[MicroPartitionIndex])
    kryo.register(classOf[Contig[MicroPartitionIndex]], new ContigSerializer[MicroPartitionIndex])

    kryo.register(classOf[Coverage])

    kryo.register(classOf[MagicHashMap[_, _]])

    // Germline-assembly caller flatmaps some CalledAlleles.
    kryo.register(classOf[Array[CalledAllele]])
    kryo.register(classOf[CalledAllele])
    kryo.register(classOf[Genotype])
    kryo.register(classOf[Allele])
    kryo.register(classOf[AlleleEvidence])

    // Seems to be used when collecting RDDs of Objects of various kinds.
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])

    // https://mail-archives.apache.org/mod_mbox/spark-user/201504.mbox/%3CCAC95X6JgXQ3neXF6otj6a+F_MwJ9jbj9P-Ssw3Oqkf518_eT1w@mail.gmail.com%3E
    kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1"))
    kryo.register(classOf[java.lang.Class[_]])

    kryo.register(classOf[RichVariant])
    kryo.register(classOf[VariantContext])
    kryo.register(classOf[Array[String]])

    kryo.register(classOf[Array[CalledSomaticAllele]])
    kryo.register(classOf[CalledSomaticAllele])

    new reference.Registrar().registerClasses(kryo)

    Position.registerKryo(kryo)

    new bases.Registrar().registerClasses(kryo)
    kryo.register(classOf[Base])
    kryo.register(classOf[Array[Base]])

    kryo.register(classOf[Path], new PathSerializer)
  }
}
