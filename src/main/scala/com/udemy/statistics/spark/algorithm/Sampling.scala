/*
Copyright 2018 Udemy, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.udemy.statistics.spark.algorithm

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.commons.lang3.RandomUtils.nextLong

import scala.annotation.tailrec
import scala.Numeric.Implicits._
import scala.reflect.ClassTag

object Sampling {

  object BootstrapStatistic extends Enumeration {
    type Statistic = Value
    val Mean: Statistic = Value
  }

  def bootstrap[T: Numeric](spark: SparkSession,
                            data: Dataset[T],
                            draws: Int,
                            statistic: BootstrapStatistic.Statistic = BootstrapStatistic.Mean,
                            useFrequencies: Boolean = true)
                           (implicit c: ClassTag[T]): Dataset[Double] = {
    import spark.implicits._

    case class CumulativeFrequency(value: T, upper: Long)
    case class Frequency(value: T, frequency: Long)

    sealed trait Bootstrappable {
      def apply(index: Long): Option[T]
      val size: Long
    }

    case class BootstrappableCumulativeFreqs(cumulativeFreqs: List[CumulativeFrequency], size: Long) extends Bootstrappable {
      def apply(index: Long): Option[T] = {
        @tailrec
        def getValue(cumulativeFreqs: List[CumulativeFrequency], index: Long): Option[T] = cumulativeFreqs match {
          case Nil => None
          case CumulativeFrequency(value, upper) :: tail =>
            if (index < upper) Some(value)
            else getValue(tail, index)
        }
        if (index < 0 || index >= size) None
        else getValue(cumulativeFreqs, index)
      }
    }

    case class BootstrappableHybrid(mostFrequentValue: CumulativeFrequency,
                                    remainingValues: Array[T],
                                    size: Long) extends Bootstrappable {
      def apply(index: Long): Option[T] = {
        if (index < 0 || index >= size) None
        else if (index < mostFrequentValue.upper) Some(mostFrequentValue.value)
        else remainingValues.lift((index - mostFrequentValue.upper).toInt)
      }
    }

    def optimalBootstrappable(data: Dataset[T]): Bootstrappable = {
      val size = data.count
      val bootstrappable = {
        if (size < 1) BootstrappableCumulativeFreqs(List.empty, size)
        else if (useFrequencies) {
          val frequencies = data.rdd
            .map((_, 1))
            .reduceByKey(_ + _)
            .map(x => Frequency(x._1, x._2)).collect.toList
          val cumulativeFreqs = buildSortedCumulativeFrequencies(frequencies)

          BootstrappableCumulativeFreqs(cumulativeFreqs, size)
        }
        else {
          val mostFrequentValueAndCount = data.rdd
            .map((_, 1)).reduceByKey(_ + _)
            .reduce((val1, val2) => if (val1._2 > val2._2) val1 else val2)
          val mostFrequentValue = CumulativeFrequency(mostFrequentValueAndCount._1, mostFrequentValueAndCount._2)
          val remainingValues = data.filter(_ != mostFrequentValue.value).collect
          BootstrappableHybrid(mostFrequentValue, remainingValues, size)
        }
      }
      bootstrappable
    }

    def bootstrapMean(bootstrappable: Bootstrappable): Double = {
      val size = bootstrappable.size
      if (size < 1) Double.NaN
      else {
        @tailrec
        // This has a potential for overflow, should be refactored to take that into consideration
        def sumValues(current: Long, acc: Double = 0D): Double = {
          if (current == 0) acc
          else {
            val value = bootstrappable(nextLong(0, size)) match {
              case Some(num) => num.toDouble
              case None => Double.NaN
            }
            sumValues(current - 1, acc + value)
          }
        }
        sumValues(size) / size
      }
    }

    def buildSortedCumulativeFrequencies(frequencies: List[Frequency]): List[CumulativeFrequency] = {
      @tailrec
      def buildList(freqs: List[Frequency],
                    acc: List[CumulativeFrequency] = List.empty,
                    currMax: Int = 0): List[CumulativeFrequency] = freqs match {
        case Nil => List.empty
        case Frequency(value, freq) :: Nil =>
          CumulativeFrequency(value, currMax + freq.toInt) :: acc
        case Frequency(value, freq) :: tail =>
          buildList(tail, CumulativeFrequency(value, currMax + freq.toInt) :: acc, currMax + freq.toInt)
      }
      val result = buildList(frequencies.sortWith(_.frequency > _.frequency))
      result.reverse
    }

    if (draws < 1) spark.emptyDataset[Double]
    else {
      val b = spark.sparkContext.broadcast(optimalBootstrappable(data))
      val bootstraps = spark.sparkContext.parallelize(1 to draws).map(_ => bootstrapMean(b.value)).toDS
      b.unpersist()
      bootstraps
    }
  }
}
