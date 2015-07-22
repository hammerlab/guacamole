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

package org.hammerlab.guacamole.commands

import javax.script.{ CompiledScript, Compilable, SimpleBindings, Bindings }

import org.apache.spark.SparkContext
import org.hammerlab.guacamole.Common.Arguments.{ NoSequenceDictionary }
import org.hammerlab.guacamole.commands.Evaluation.{ Environment, PileupsEnvironment, CodeWithEnvironment }
import org.hammerlab.guacamole.pileup.{ PileupElement, Pileup }
import org.hammerlab.guacamole.reads.{ MappedRead, Read }
import org.hammerlab.guacamole._
import org.kohsuke.args4j.{ Option => Args4jOption, Argument }

import scala.collection
import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable

object EvalCommand {

  protected class Arguments extends DistributedUtil.Arguments with NoSequenceDictionary {

    @Argument(required = true, multiValued = true,
      usage = "[label1: ]bam1 ... - [header1: ]expression1 ... ")
    var fields: Array[String] = Array.empty

    @Args4jOption(name = "--out",
      usage = "Output path. Default: stdout")
    var out: String = ""

    @Args4jOption(name = "--include-empty",
      usage = "Include empty pileups.")
    var includeEmpty: Boolean = false

    @Args4jOption(name = "--filter",
      usage = "Filter expression.")
    var filter: String = ""

    @Args4jOption(name = "--separator",
      usage = "CSV field separator")
    var separator: String = ", "

    @Args4jOption(name = "--domain",
      usage = "Input to evaluate, either: 'reads' or 'pileups' (default)")
    var domain: String = "pileups"
  }

  object Caller extends SparkCommand[Arguments] {
    override val name = "eval"
    override val description = "run javascript code over pileups"

    def parseFields(fields: Seq[String]): (Seq[String], Seq[String], Seq[String], Seq[String]) = {
      val bams = ArrayBuffer.newBuilder[String]
      val bamLabels = ArrayBuffer.newBuilder[String]
      val expressions = ArrayBuffer.newBuilder[String]
      val expressionLabels = ArrayBuffer.newBuilder[String]
      var label = ""
      var parsingExpressions = false
      var nextDefaultNum = 1
      fields.foreach(fieldRaw => {
        val field = fieldRaw.trim
        if (field == "::") {
          if (parsingExpressions) {
            throw new RuntimeException("Couldn't parse fields: multiple separator (=>) arguments")
          } else if (label.nonEmpty) {
            throw new RuntimeException("Couldn't parse fields: no value for label: " + label)
          } else {
            parsingExpressions = true
          }
        } else {
          if (label.nonEmpty) {
            if (parsingExpressions) {
              expressions += fieldRaw
              expressionLabels += label
            } else {
              bams += fieldRaw
              bamLabels += label
            }
            label = ""
          } else {
            if (field.endsWith(":")) {
              label = field.dropRight(1)
            } else {
              if (parsingExpressions) {
                expressions += field
                expressionLabels += field
              } else {
                bams += field
                bamLabels += "r%d".format(nextDefaultNum)
                nextDefaultNum += 1
              }
            }
          }
        }
      })
      (bamLabels.result, bams.result, expressionLabels.result, expressions.result)
    }

    override def run(args: Arguments, sc: SparkContext): Unit = {
      val (bamLabels, bams, expressionLabels, expressions) = parseFields(args.fields)
      assert(bamLabels.size == bams.size)
      assert(expressionLabels.size == expressions.size)

      println("Read inputs (%d):".format(bams.size))
      bams.zip(bamLabels).foreach(pair => println("%20s : %s".format(pair._2, pair._1)))
      println()
      println("Expressions (%d):".format(expressions.size))
      expressions.zip(expressionLabels).foreach(pair => println("%20s : %s".format(pair._2, pair._1)))
      println()

      val readSets = bams.map(
        path => ReadSet(
          sc, path, true, Read.InputFilters.empty, token = 0, contigLengthsFromDictionary = !args.noSequenceDictionary))

      assert(readSets.forall(_.sequenceDictionary == readSets(0).sequenceDictionary),
        "Samples have different sequence dictionaries: %s."
          .format(readSets.map(_.sequenceDictionary.toString).mkString("\n")))

      val loci = Common.loci(args, readSets(0))
      val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, loci, readSets.map(_.mappedReads): _*)

      val separator = args.separator
      val result = DistributedUtil.pileupFlatMapMultipleRDDsWithState[String, Option[Seq[CodeWithEnvironment[Seq[Pileup]]]]](
        readSets.map(_.mappedReads),
        lociPartitions,
        !args.includeEmpty, // skip empty
        initialState = None,
        (maybeState, pileups) => {
          val codeWithEnvironments = maybeState match {
            case None => expressions.map(
              expression =>
                new CodeWithEnvironment[Seq[Pileup]](expression, new PileupsEnvironment(bamLabels)))
            case Some(state) => state
          }
          val results = codeWithEnvironments.map(e => e(pileups))
          if (results.exists(_ == "--skip--"))
            (Some(codeWithEnvironments), Iterator.empty)
          else
            (Some(codeWithEnvironments), Iterator.single(results.mkString(separator)))
        })

      if (args.out.nonEmpty) {
        sc.parallelize(Seq(expressionLabels.mkString(separator))).union(result).saveAsTextFile(args.out)
      } else {
        println("BEGIN RESULT")
        println("*" * 60)
        println(expressionLabels.mkString(separator))
        result.collect.foreach(println _)
        println("*" * 60)
        println("END RESULT")
      }

      DelayedMessages.default.print()
    }
  }
}
object Evaluation {
  val factory = new javax.script.ScriptEngineManager
  val engine = factory.getEngineByName("JavaScript")
  val compilingEngine = engine.asInstanceOf[Compilable]

  val pileupElementCodeWithEnvironmentCache = new collection.mutable.HashMap[String, CodeWithEnvironment[PileupElement]]
  def countPileupElements(pileup: Pileup, code: String): Int = {
    if (code.isEmpty) {
      pileup.elements.size
    } else {
      val evaluator = if (pileupElementCodeWithEnvironmentCache.contains(code)) {
        pileupElementCodeWithEnvironmentCache(code)
      } else {
        val evaluator = new CodeWithEnvironment[PileupElement](code, new PileupElementEnvironment())
        pileupElementCodeWithEnvironmentCache.put(code, evaluator)
        evaluator
      }
      var count = 0
      pileup.elements.foreach(element => {
        PartialFunction.condOpt(evaluator(element): Any) {
          case b: Boolean => if (b) count += 1 else {}
          case _          => throw new RuntimeException("Expected a boolean return value.")
        }
      })
      count
    }
  }

  abstract class Environment[T] {
    val prologueBuilder = ArrayBuffer.newBuilder[String]

    def include[S](other: Environment[S]): Unit = {
      prologueBuilder ++= other.prologueBuilder.result
    }

    def addPrologueFunction(name: String): Unit = {
      prologueBuilder += "function %s(value) { return _%s.apply(value); };".format(name, name)
    }
    def addBindings(value: T, bindings: Bindings): Unit
    lazy val prologue = prologueBuilder.result.mkString("\n")
  }

  case class CodeWithEnvironment[T](code: String, environment: Environment[T]) {
    val fullCode = environment.prologue + "\nskip = '--skip--'\n" + code
    val compiledCode = compilingEngine.compile(fullCode)
    val cachedBindings = new SimpleBindings()

    def apply(value: T): Any = {
      environment.addBindings(value, cachedBindings)

      try {
        compiledCode.eval(cachedBindings)
      } catch {
        case e: javax.script.ScriptException => {
          val codeDisplay = fullCode.split("\n").zipWithIndex.map(
            pair => "\t%3d| %s".format(pair._2 + 1, pair._1)).mkString("\n")
          throw new javax.script.ScriptException(e.getMessage
            + " while evaluating:\n" + codeDisplay + "\nwith these bindings:\n\t"
            + JavaConversions.asScalaSet(
              cachedBindings.keySet).map(key => "%20s = %20s".format(key, cachedBindings.get(key))).mkString("\n\t"),
            e.getFileName,
            e.getLineNumber,
            e.getColumnNumber)
        }
      }
    }
  }

  class ReadEnvironment(prefix: String = "") extends Environment[MappedRead] {
    def addBindings(read: MappedRead, result: Bindings): Unit = {
      result.put("baseQualities", read.baseQualities)
      result.put("isMapped", read.isMapped)
      result.put("isDuplicate", read.isDuplicate)
      result.put("alignmentLikelihood", read.alignmentLikelihood)

      result.put("start", read.start)
      result.put("end", read.end)
      result.put("alignmentQuality", read.alignmentQuality)
    }
  }

  class PileupElementEnvironment(prefix: String = "") extends Environment[PileupElement] {
    val readEnvironment = new ReadEnvironment(prefix = prefix)
    include(readEnvironment)

    def addBindings(element: PileupElement, result: Bindings): Unit = {
      result.put("quality", element.qualityScore)
      result.put("isDeletion", element.isDeletion)
      result.put("isInsertion", element.isInsertion)
      result.put("isMatch", element.isMatch)
      result.put("isMidDeletion", element.isMidDeletion)
      result.put("isMismatch", element.isMismatch)
      result.put("indexWithinCigarElement", element.indexWithinCigarElement)
      result.put("sequencedBases", Bases.basesToString(element.sequencedBases))
      readEnvironment.addBindings(element.read, result)
    }
  }

  class PileupEnvironment(prefix: String = "") extends Environment[Pileup] {
    addPrologueFunction(prefix + "count")
    def addBindings(pileup: Pileup, bindings: Bindings): Unit = {
      bindings.put(prefix + "ref", Bases.baseToString(pileup.referenceBase))
      bindings.put(prefix + "locus", pileup.locus)
      bindings.put(prefix + "contig", pileup.referenceName)
      def count(expression: String): Int = {
        if (expression.isEmpty) {
          pileup.elements.size
        } else {
          countPileupElements(pileup, expression)
        }
      }
      bindings.put("_" + prefix + "count", count _)
    }
  }

  class PileupsEnvironment(labels: Seq[String], prefix: String = "") extends Environment[Seq[Pileup]] {
    val pileupEnvironments = labels.map(label => new PileupEnvironment(prefix = "%s%s_".format(prefix, label)))
    pileupEnvironments.foreach(include _)
    def addBindings(pileups: Seq[Pileup], bindings: Bindings): Unit = {
      bindings.put(prefix + "ref", Bases.baseToString(pileups(0).referenceBase))
      bindings.put(prefix + "locus", pileups(0).locus)
      bindings.put(prefix + "contig", pileups(0).referenceName)
      bindings.put(prefix + "pileups", pileups)
      pileups.zip(pileupEnvironments).foreach(pair => pair._2.addBindings(pair._1, bindings))
    }
  }
}
