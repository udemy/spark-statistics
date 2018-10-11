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

import com.udemy.statistics.spark.algorithm.Sampling.{bootstrap, BootstrapStatistic}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class SamplingSpec extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  private val spark = SparkSession.builder().master("local").appName("spark session").getOrCreate()
  import spark.implicits._

  "bootstrap" should "draw bootstrap means from Dataset of numeric values" in {
    forAll (Gen.listOf[Double](Arbitrary.arbDouble.arbitrary), Gen.posNum[Int], Arbitrary.arbBool.arbitrary) {
      (data: List[Double], draws: Int, useFrequencies: Boolean) =>
        val dataset = spark.sparkContext.parallelize(data).toDS
        val output = bootstrap(spark, dataset, draws, BootstrapStatistic.Mean, useFrequencies).collect

        output.length shouldEqual draws
        if (data.isEmpty) {
          output.foreach(_.isNaN shouldBe true)
        }
        else {
          val min = data.min
          val max = data.max
          output.foreach{x =>
            x should be >= min
            x should be <= max
          }
        }
    }
  }

  it should "return empty Dataset when number of draws in less than 1" in {
    forAll (Gen.listOf[Double](Arbitrary.arbDouble.arbitrary), Gen.choose(Int.MinValue, 0)) {
      (data: List[Double], draws: Int) =>
        val dataset = spark.sparkContext.parallelize(data).toDS
        bootstrap(spark, dataset, draws).collect.isEmpty shouldBe true
    }
  }
}
