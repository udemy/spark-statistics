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

name := "com.udemy.statistics.spark"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-lang3" % "3.+",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.scalatest" % "scalatest_2.11" % "3.0.+" % "test",
  "org.scalacheck" %% "scalacheck" % "1.13.+" % "test"
)
