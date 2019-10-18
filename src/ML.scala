import java.io.File

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.{Vectors, Vector => MLVector}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.JavaConverters._

object ML extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("yjslol-mllib")
      .getOrCreate()
    val sc = spark.sparkContext
    case class Game(sid: String, rank: Double, sname: String, gid: String, kill: Double, death: Double, assist: Double, wards: Double, csPerMin: Double)
    case class GameFactors(value: String, features: Vector[Double])
    var df: DataFrame = null

    if (new File("model/games-data").exists()) {
      println("Read data from model/games-data")
      df = spark.read.parquet("model/games-data").toDF("sid", "features")
    } else {
      println("Aggregate new data")
      val games = MongoDBReceiver.getGames.asScala.map(t => Game(t._1, t._2.toDouble, t._3, t._4, t._5.toDouble, t._6.toDouble, t._7.toDouble, t._8.toDouble, t._9))
      val gRDD = sc.parallelize(games).map(g => (g.sid, (g.kill, g.death, g.assist, g.wards, g.csPerMin, 1)))
        .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6))
        .map { g =>
          val cnt = g._2._6.toDouble //game count
        val pfm = g._2 //performance tuple
          (g._1, Vectors.dense(pfm._1 / cnt, pfm._2 / cnt, pfm._3 / cnt, pfm._4 / cnt, pfm._5 / cnt))
        }
      //      println(gRDD.first())
      val garr: Array[(String, MLVector)] = gRDD.collect()
      val gseq: Seq[(String, MLVector)] = garr.toSeq
      df = spark.createDataFrame(gseq).toDF("sid", "features")

      //      df = spark.createDataFrame(gRDD, schema)
      //    df.write.json("model/games-data")
      df.write.mode(SaveMode.Overwrite).parquet("model/games-data")
    }

    df.printSchema()

    //    val costs = Seq(5).map {
    //      k => (k, new ClusteringEvaluator().evaluate(new KMeans().setK(k).setSeed(1L).fit(df).transform(df)))
    //    }
    //    costs.foreach {
    //      case (k, e) => println(s"k = $k's Silhouette with squared euclidean distance = $e")
    //    }
    var model: PipelineModel = null
    if (new File("model/games-kmeans-5-model").exists()) {
      println("Load model from model/games-kmeans-5-model")
      model = PipelineModel.load("model/games-kmeans-5-model")
    } else {
      println("Train new model")
      val pca = new PCA().setInputCol("features").setOutputCol("pcaFeatures").setK(2)
      val km = new KMeans().setK(5).setSeed(1L).setFeaturesCol("pcaFeatures")

      val pipeline = new Pipeline().setStages(Array(pca, km))
      model = pipeline.fit(df)
      model.write.overwrite().save("model/games-kmeans-5-model")
    }

    val predictions = model.transform(df)


    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    val predictResult = predictions.select("sid", "features", "pcaFeatures", "prediction")

    predictResult.collect()
      .foreach {
        case Row(sid: String, feature: MLVector, pcaFeatures: MLVector, prediction: Int) =>
          println(s"($sid,$feature) -> $pcaFeatures -> $prediction")
      }

    predictResult.write.mode(SaveMode.Overwrite).save("model/pca-kmeans-result")
    //    println("Cluster Centers: ")
    //    model.clusterCenters.foreach(println)

    spark.close()
  }
}
