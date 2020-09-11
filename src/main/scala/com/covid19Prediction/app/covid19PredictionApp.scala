package com.covid19Prediction.app

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.types.FloatType


object covid19PredictionApp {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("covid19PredictionApp")
      .master("local[*]")
      .getOrCreate()
    val rawData = spark.read.format("csv")
      .option("header", "true")
      .load("/home/nihad/machine_learning/patient_data/*.csv")


    val dataSet = rawData.select(
      col("Current Status").as("status"),
      col("Age Bracket").cast(FloatType).as("age"),
      col("Gender").as("sex"),
      col("State code").as("state")
    ).na.drop("any")
      .filter(
        col("status").notEqual("Hospitalized") &&
          col("state").notEqual("UN")
      ).cache()


    //val Array(training_data, test_data) = dataSet.randomSplit(Array(0.8, 0.2))
    val model = new train(dataSet)

    val test_data = new dataGenerator(spark).DF
    val predictionDF = model.predict(test_data).cache()


    val probabilityUnderGender = udf((x: Vector) => x(0)*100)
    val probabilityUnderAge = udf((x: Vector) => x(1)*100)
    val probabilityUnderState = udf((x: Vector) => x(2)*100)


    val probabilityDF = predictionDF.withColumn("p_sex", probabilityUnderGender(col("probability")))
      .withColumn("p_age", probabilityUnderAge(col("probability")))
      .withColumn("p_state", probabilityUnderState(col("probability")))
      .select(
      col("sex"),
      col("age"),
      col("state"),
      col("p_sex"),
        col("p_age"),
        col("p_state")
    )
    probabilityDF.show(10, truncate = false)

    probabilityDF.write.format("org.apache.spark.sql.cassandra")
        .options(Map("keyspace" -> "covid19", "table" -> "prediction"))
        .mode("APPEND")
        .save()
    spark.close()
  }
}
