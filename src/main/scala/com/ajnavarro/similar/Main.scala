package com.ajnavarro.similar

import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import tech.sourced.engine._

object Main extends App {
  val repositoriesPathKey = "spark.tech.sourced.engine.repositories.path"
  val rootedRepoPath = "/home/antonio/root-repositories-100"
  val tokensParquetFile = "/home/antonio/root-repositories-100/OUT/tokens.parquet"
  val outCsvFile = "/home/antonio/root-repositories-100/OUT/out.csv"
  val spark = SparkSession.builder.appName("similarity-files").master("local[8]").getOrCreate()

  import spark.implicits._

  val conf = spark.sparkContext.hadoopConfiguration
  val fs = org.apache.hadoop.fs.FileSystem.get(conf)
  val exists = fs.exists(new org.apache.hadoop.fs.Path(tokensParquetFile))

  if (!exists) {
    println("GENERATING TOKENS")
    generateTokens()
  }

  val resultDf = spark.read.parquet(tokensParquetFile)

  resultDf.show()

  val vocabSize = 1000000
  val cvModel = new CountVectorizer()
    .setInputCol("tokens")
    .setOutputCol("features")
    .setVocabSize(vocabSize)
    .setMinDF(10).fit(resultDf)

  val isNoneZeroVector = functions.udf({ v: Vector => v.numNonzeros > 0 }, DataTypes.BooleanType)
  val vectorizedDf = cvModel.transform(resultDf).filter(isNoneZeroVector(col("features")))
    .drop("content", "tokens", "commit_hash")

  val mh = new MinHashLSH().setNumHashTables(3).setInputCol("features").setOutputCol("hashValues")
  val model = mh.fit(vectorizedDf)

  val threshold = 0.8
  val modelDf = model.approxSimilarityJoin(vectorizedDf, vectorizedDf, threshold).filter("distCol != 0")
    .orderBy(functions.desc("distCol"))

  modelDf.printSchema()

  val modelOutputDf = modelDf
    .withColumn("repoA", $"datasetA.repository_id")
    .withColumn("pathA", $"datasetA.path")
    .withColumn("repoB", $"datasetB.repository_id")
    .withColumn("pathB", $"datasetB.path")
    .drop("datasetA", "datasetB")

  modelOutputDf.show(false)

  modelOutputDf.repartition(1).write.mode(SaveMode.Overwrite).csv(outCsvFile)

  private def generateTokens(): Unit = {
    val engine = Engine(spark, rootedRepoPath)

    val reposDf = engine.getRepositories
    val referencesDf = getDataSource("references", spark)
    val refsAndRepos = referencesDf.join(reposDf, referencesDf("repository_id") === reposDf("id"))

    val firstHeadCommitDf = refsAndRepos.getHEAD.getCommits.filter("index == 0")

    val filesDf = getDataSource("files", spark)

    val headFilesDf = filesDf.join(firstHeadCommitDf, filesDf("commit_hash") === firstHeadCommitDf("hash"))
      .drop(firstHeadCommitDf("hash"))

    val prunedFilesDf = headFilesDf.filter("is_binary = false")
      .drop("index", "parents", "tree", "blobs", "parents_count", "message", "author_date", "committer_name", "reference_name")

    val filteredFilesDf = prunedFilesDf.classifyLanguages.filter("lang = 'Python' OR lang = 'Java'")
      .withColumn("size", functions.length(functions.col("content")))

    val resultDf = filteredFilesDf
      .extractUASTs()
      .queryUAST("//*[@roleIdentifier and not(@roleIncomplete)]")
      .extractTokens()
      .drop("is_binary", "uast", "result", "content")

    resultDf.show()
    resultDf.write.mode(SaveMode.Overwrite).parquet(tokensParquetFile)
  }

  private def getDataSource(table: String, session: SparkSession): DataFrame =
    session.read.format("tech.sourced.engine.DefaultSource")
      .option("table", table)
      .load(session.sqlContext.getConf(repositoriesPathKey))
}
