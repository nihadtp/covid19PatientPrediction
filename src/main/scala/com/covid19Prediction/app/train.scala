package com.covid19Prediction.app

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{Dataset, Row}

class train(dataset: Dataset[Row]) extends Serializable {

  private final val stringColumns = Array("status" ,"sex", "state")

  private var transformedDF: Dataset[Row] = _

  private val stringIndexerArray = stringColumns.map(col => {
    new StringIndexer()
      .setInputCol(col)
      .setOutputCol(col + "_indexed")
      .setHandleInvalid("keep")
  })

  private val stringEncoder = new OneHotEncoderEstimator()
    .setInputCols(stringColumns.map(col => col + "_indexed"))
    .setOutputCols(stringColumns.map(col => col + "_encoded"))
    .setHandleInvalid("keep")

  private val vectorize = new VectorAssembler()
    .setInputCols(Array("age", "sex_encoded", "state_encoded"))
    .setOutputCol("features")
    .setHandleInvalid("keep")

  private val randomClassifier = new RandomForestClassifier()
    .setLabelCol("status_indexed")
    .setFeaturesCol("features")
    .setPredictionCol("prediction")
    .setMaxDepth(5)

  private val model = new Pipeline()
    .setStages(
      stringIndexerArray ++
        Array(
          stringEncoder,
          vectorize,
          randomClassifier
        )
    ).fit(dataset)

  def predict(dataset: Dataset[Row]): Dataset[Row] = {
    transformedDF = model.transform(dataset)
    transformedDF
  }

  def getEvaluation(): Double = {
    new MulticlassClassificationEvaluator()
      .setLabelCol("status_indexed")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
      .evaluate(transformedDF)
  }
}
