guacamole
=========

Guacamole is an ADAM-based variant calling framework inspired by Avocado.

The goal is a small, readable codebase that can be understood quickly and adapted for experimentation.


## Running Guacamole

Build:

```
mvn package
```

Run:

```
scripts/guacamole threshold \
	-loci chrM:2000-2050 \
	-reads guacamole-core/src/test/resources/chrM.sorted.bam \
	-threshold 0 \
	-out /tmp/OUT.gt.adam \
	-spark_kryo_buffer_size 64
```

You can then inspect the results, for example, using ADAM's summarize genotypes command:

```
adam summarize_genotypes /tmp/OUT.gt.adam
```

