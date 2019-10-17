import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}

import scala.collection.JavaConverters._

object ML extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("yjslol-mllib")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    case class Game(sid: String, rank: Int, sname: String, gid: String, kill: Int, death: Int, assist: Int, wards: Int, csPerMin: Double)
    case class GameFactors(value: String, features: Vector[Double])
    val games = MongoDBReceiver.getGames.asScala.map(t => Game(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9))
    val gRDD = sc.parallelize(games).map(g => ((g.sid, g.gid), (g.kill, g.death, g.assist, g.wards, g.csPerMin, 1)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6))
      .map { g =>
        val cnt = g._2._6 //game count
      val pfm = g._2 //performance tuple
        Row(g._1._1, Vector(pfm._1 / cnt, pfm._2 / cnt, pfm._3 / cnt, pfm._4 / cnt, pfm._5 / cnt))
      }
    println(gRDD.first())

    val schemaString = "value features"

    // Generate the schema based on the string of schema
//    val fields = schemaString.split(" ")
//      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val fields = Seq(StructField("value",StringType,nullable = false),StructField("features",ArrayType(DoubleType,containsNull = false),nullable = false))
    val schema = StructType(fields)

    val df = spark.createDataFrame(gRDD,schema)

//    implicit val encoder: Encoder[GameFactors] = Encoders.kryo[GameFactors]
//    val ds = df.as[GameFactors]
    df.printSchema()

    val costs = Seq(2,3,4,5,6,7,8).map{
      k => (k,new ClusteringEvaluator().evaluate(new KMeans().setK(k).setSeed(1L).fit(df).transform(df)))
    }
    costs.foreach{
      case(k,e) => println(s"k = $k's Silhouette with squared euclidean distance = $e")
    }
//    val kmeans = new KMeans().setK(5).setSeed(1L)
//    val model = kmeans.fit(df)
//
//    val predictions = model.transform(df)
//
//    val evaluator = new ClusteringEvaluator()
//
//    val silhouette = evaluator.evaluate(predictions)
//    println(s"Silhouette with squared euclidean distance = $silhouette")
//
//    // Shows the result.
//    println("Cluster Centers: ")
//    model.clusterCenters.foreach(println)
  }
}
