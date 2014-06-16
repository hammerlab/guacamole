guacamole
=========
[![Build Status](https://travis-ci.org/hammerlab/guacamole.svg?branch=master)](https://travis-ci.org/hammerlab/guacamole)
Guacamole is a Spark-based variant calling framework inspired by Avocado.

The goal is a small, readable codebase that can be understood quickly and adapted for experimentation.

Currently, the variant callers included are toy implementations for understanding Spark performance, and are not suitable
for production use.


## Running Guacamole

Build:

```
mvn package
```

Run:

```
scripts/guacamole threshold \
	-reads src/test/resources/chrM.sorted.bam \
	-out /tmp/OUT.vcf
```

Try 
```
scripts/guacamole -h
```
for a list of implemented variant callers, or

```
scripts/guacamole <caller> -h
```
for help on a particular variant caller.


