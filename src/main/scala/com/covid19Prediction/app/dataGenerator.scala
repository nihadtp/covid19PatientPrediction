package com.covid19Prediction.app

import org.apache.spark.sql.SparkSession

class dataGenerator(spark: SparkSession) extends Serializable {

  final private val ageRange = 10 to 90
  final private val genderList = List("M", "F")
  final private val stateCodeList = stateCode.stateMap.keys.toList
  private var rowList = scala.collection.mutable.ListBuffer[(Float, String, String, String)]()

  import spark.implicits._
  for(
    gender <- genderList;
    state <- stateCodeList;
    age <- ageRange
  ){
    rowList += ((age.toFloat, state, gender, "No Data"))
  }

  val DF = rowList.toDF("age", "state", "sex", "status")

}
