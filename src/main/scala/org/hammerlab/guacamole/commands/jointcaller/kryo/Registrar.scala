package org.hammerlab.guacamole.commands.jointcaller.kryo

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.guacamole.commands.jointcaller.Parameters.SomaticGenotypePolicy
import org.hammerlab.guacamole.commands.jointcaller.annotation.{InsufficientNormal, MultiSampleAnnotations, SingleSampleAnnotations, StrandBias}
import org.hammerlab.guacamole.commands.jointcaller.evidence.{MultiSampleMultiAlleleEvidence, MultiSampleSingleAlleleEvidence, NormalDNASingleSampleSingleAlleleEvidence, TumorDNASingleSampleSingleAlleleEvidence, TumorRNASingleSampleSingleAlleleEvidence}
import org.hammerlab.guacamole.commands.jointcaller.{AlleleAtLocus, Input, InputCollection, Parameters}

class Registrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[NormalDNASingleSampleSingleAlleleEvidence])
    kryo.register(classOf[TumorDNASingleSampleSingleAlleleEvidence])

    // These are needed for MultiSampleSingleAlleleEvidence; perTumorDnaSampleTopMixtures and probably other fields.
    kryo.register(Class.forName("scala.collection.immutable.MapLike$$anon$2"))  // MapLike.MappedValues?
    kryo.register(Class.forName("org.hammerlab.guacamole.commands.jointcaller.evidence.MultiSampleSingleAlleleEvidence$$anonfun$16"))
    kryo.register(Class.forName("org.hammerlab.guacamole.commands.jointcaller.evidence.MultiSampleSingleAlleleEvidence$$anonfun$10"))

    kryo.register(classOf[Parameters])
    kryo.register(Class.forName("scala.Enumeration$Val"))
    kryo.register(SomaticGenotypePolicy.getClass)
    kryo.register(classOf[MultiSampleAnnotations])
    kryo.register(classOf[InputCollection])
    kryo.register(classOf[Input])
    kryo.register(Input.Analyte.getClass)
    kryo.register(Input.TissueType.getClass)
    kryo.register(classOf[InsufficientNormal])
    kryo.register(classOf[TumorRNASingleSampleSingleAlleleEvidence])
    kryo.register(classOf[Parameters.SomaticGenotypePolicy.Value])
    kryo.register(classOf[StrandBias])
    kryo.register(classOf[SingleSampleAnnotations])
    kryo.register(classOf[AlleleAtLocus])
    kryo.register(classOf[MultiSampleSingleAlleleEvidence])
    kryo.register(classOf[MultiSampleMultiAlleleEvidence])
    kryo.register(classOf[Array[MultiSampleMultiAlleleEvidence]])
  }
}
