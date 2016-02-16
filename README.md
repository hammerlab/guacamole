guacamole
=========
[![Build Status](https://travis-ci.org/hammerlab/guacamole.svg?branch=master)](https://travis-ci.org/hammerlab/guacamole)

Guacamole is a framework for variant calling, i.e. identifying DNA mutations
from [Next Generation Sequencing][seq] data. It currently includes a toy
germline (non-cancer) variant caller as well as a somatic variant caller for
finding cancer mutations.  Most development effort has gone into somatic
calling.

The emphasis is on a readable codebase that can be readily understood and
adapted for experimentation.

Guacamole is written in Scala using the [Apache Spark][spark] engine for
distributed processing. It can run on a single computer or on a Hadoop cluster.

Guacamole supports reading aligned reads as:
 * [BAM or SAM files][sambam]
 * [Parquet files][parquet] of [bdg-formats][] [AlignmentRecords][]
 
It can write the called genotypes as:
 * [VCF files][]
 * Parquet files of bdg-formats [Genotypes][].

Guacamole uses ideas and some functionality from [ADAM][]. It also takes
inspiration from the [Avocado][] project.

For hacking Guacamole, see our [code docs][].

# Running Guacamole on a Single Node

Guacamole requires [Apache Maven][maven] (version 3.0.4 or higher).

Build:

```
mvn package
```

This will build a guacamole JAR file in the `target` directory. You can use the
`guacamole` script to run locally:

```
scripts/guacamole somatic-joint \
    src/test/resources/cancer-wgs1/normal.bam \
    src/test/resources/cancer-wgs1/primary.bam \
    src/test/resources/cancer-wgs1/recurrence.bam \
    --reference-fasta src/test/resources/hg19.partial.fasta \
    --reference-fasta-is-partial \
    --loci chr11,chr5,chr22,chr8,chr19,chr1,chr15,chr12,chr18,chr20,chr2,chr13,chr7,chr14,chr3,chr17,chr4,chr6,chr9,chrX,chr10,chr21,chr16 \
    --out-dir /tmp/test-guacamole-vcfs
```
This will call germline and somatic small variants from a set of test BAMs
using the `somatic-joint` caller, which can call variants from any number of
samples from the same patient. (The `--loci` argument is needed due to a
peculiarity of our test dataset, and is not necessary in general. Also, in a
production run, you would want to pass a regular fasta file with the
`--reference-fasta` argument and omit the `--reference-fasta-is-partial`
option.)

Try 
```
scripts/guacamole -h
```
for a list of implemented variant callers, or

```
scripts/guacamole <caller> -h
```
for help on a particular variant caller.

# Running Guacamole on a Hadoop Cluster

See Guacamole's [pom.xml](/pom.xml) file for the versions of Hadoop and Spark
that Guacamole expects to find on your cluster.

Here is an example command to get started using Guacamole in Spark's yarn
cluster mode. You'll probably have to modify it for your environment. 

```
spark-submit \
	--master yarn \
	--deploy-mode cluster \
	--driver-java-options -Dlog4j.configuration=/path/to/guacamole/scripts/log4j.properties \
	--executor-memory 4g \
	--driver-memory 10g \
	--num-executors 1000 \
	--executor-cores 1 \
	--class org.hammerlab.guacamole.Guacamole \
	--verbose \
	/path/to/target/guacamole-with-dependencies-<x.y.z>.jar \
	germline-threshold \
        --reads hdfs:///path/to/reads.bam \
        --out hdfs:///path/to/result.vcf
```

# Is this ready for production use?

Not currently. Everything here is experimental. Please use a standard tool if
you need accurate variant calls.


# License

Guacamole is released under an [Apache 2.0 license](LICENSE.txt).

YourKit is kindly supporting this open source project with its full-featured Java Profiler.
YourKit, LLC is the creator of innovative and intelligent tools for profiling
[Java](http://www.yourkit.com/java/profiler/index.jsp) and [.NET](http://www.yourkit.com/.net/profiler/index.jsp) applications.
![image](https://cloud.githubusercontent.com/assets/455755/4988560/97757f12-6935-11e4-9270-f5fc42f9b585.png)

[seq]: http://en.wikipedia.org/wiki/DNA_sequencing
[spark]: http://spark.apache.org/
[sambam]: http://genomicsandhealth.org/our-work/work-products/file-formats-sambam
[parquet]: http://parquet.incubator.apache.org/
[bdg-formats]: https://github.com/bigdatagenomics/bdg-formats
[alignmentrecords]: https://github.com/bigdatagenomics/bdg-formats/blob/master/src/main/resources/avro/bdg.avdl#L60
[vcf files]: http://genomicsandhealth.org/our-work/work-products/file-formats-vcfbcf
[genotypes]: https://github.com/bigdatagenomics/bdg-formats/blob/master/src/main/resources/avro/bdg.avdl#L547
[adam]: https://github.com/bigdatagenomics/adam
[avocado]: https://github.com/bigdatagenomics/avocado
[code docs]: http://www.hammerlab.org/guacamole/docs/#org.hammerlab.guacamole.package
[maven]: http://maven.apache.org/
