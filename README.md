# covid19PatientDeathPredictionProbability

This project uses Random Forest classifier module in spark ml library to predict probability of recovery of a covid19 patient based on 
there age, sex and state they are residing.

##Probability of recovery of different states from age 10 to 90

![image](https://imagehosting.s3.us-east-2.amazonaws.com/covid19-prediction-Zeppelin.png)



- Here x-axis is age and y-axis probability percentage.
- Here we can notice that probability decreases as age increases.
- Also probability of recovery is different for different states duu to difference in population, civic sense, hygiene etc 
- By dividing whole dataset into 80-20 as training and test data. It is calculated to be accurate upto 93%
- Main data source are from [covid19API](https://api.covid19india.org/documentation/csv/) in CSV format(Almost 50% data did not have sufficeint information)

#### Prequisites

1. Scala 2.11.12
2. Spark 2.4.6
3. SBT 1.0
4. Cassandra 4.0
5. cqlsh 5.0.1

#### Cassandra Set up

- Log into cassandra cqlsh and create Keyspace called covid19 and table called prediction

```shell script
cqlsh> CREATE TABLE covid19.prediction(sex text, age float, state text, p_sex double, p_state double, p_state double,\ PRIMARY KEY (state_prop, country_prop, date, uuid));

```


#### Steps to build jar

1. First clone github project to a folder

2. Download csv data soircce to local covidAPI

3. In src/main/scala/covid19PredictionApp.scala file. Edit the following

```scala
val rawData = spark.read.format("csv")
      .option("header", "true")
      .load("/home/nihad/machine_learning/patient_data/*.csv")
```

Under load function function put in your correct path

2. Then build fat jar using sbt assembly plugin

```shell script
sbt assembly
```

3. In target folder you will find covid19Prediction-assembly.jar file

4. Start your cassandra service

4. Go to spark folder

```shell script
spark-submit ~/pathtoyourtargetfolder/covid19Prediction-assembly.jar
```
Here app starts and fetches data from the folder, converts them to Dataframe and trains using Random Forest classifier
And predicted values are then written to cassandra table.

