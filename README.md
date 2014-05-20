guacamole
=========
[![Build Status](https://travis-ci.org/hammerlab/guacamole.svg?branch=master)](https://travis-ci.org/hammerlab/guacamole)
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
	-reads guacamole-core/src/test/resources/chrM.sorted.bam \
	-out /tmp/OUT.gt.adam \
```

You can then inspect the results, for example, using ADAM's summarize genotypes command:

```
adam summarize_genotypes /tmp/OUT.gt.adam
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


