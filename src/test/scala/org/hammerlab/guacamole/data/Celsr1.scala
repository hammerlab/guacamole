package org.hammerlab.guacamole.data

import org.apache.hadoop.fs.Path
import org.hammerlab.guacamole.jointcaller.Inputs
import org.hammerlab.test.resources.File

object Celsr1 {
  val bams =
    Vector("normal_0.bam", "tumor_wes_2.bam", "tumor_rna_11.bam")
      .map(
        name ⇒
          new Path(File(s"cancer-wes-and-rna-celsr1/$name"))
      )

  val inputs = Inputs(bams, analytes = Vector("dna", "dna", "rna"))
}
