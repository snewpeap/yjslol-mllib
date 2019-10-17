import java.util

import com.mongodb.spark.rdd.MongoRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bson.Document

class Clustering extends Serializable {
//  def get(rdd: MongoRDD[Document]): Unit = {
//    val aggregatedRDD = rdd.withPipeline(
//      Seq(
//        new Document("$match",
//          new Document("updateAt",
//            new Document("$eq", 1570936560L))
//            .append("games",
//              new Document("$exists", true))
//            .append("rank",
//              new Document("$gt", 50))),
//        new Document("$project",
//          new Document("_id", 0L)
//            .append("sid", 1L)
//            .append("sname", 1L)
//            .append("rank", 1L)
//            .append("gs",
//              new Document("$filter",
//                new Document("input", "$games")
//                  .append("as", "g")
//                  .append("cond",
//                    new Document()
//                  )
//              ))
//        ))
//    ).persist(StorageLevel.MEMORY_ONLY_SER)
//
//    val gamesRDD: RDD[Game] = null
//    val rawRDD = aggregatedRDD.map(d => (d.get("sid").toString, d.get("rank").toString.toInt, d.get("sname").toString, d.get("gs").asInstanceOf[util.ArrayList[Document]].asScala))
//    rawRDD.collect().foreach { t4 =>
//      val sid = t4._1
//      val rank = t4._2
//      val sname = t4._3
//      gamesRDD.++(rdd.context.parallelize(t4._4.map(
//        g => Game(
//          sid, rank, sname,
//          g.get("kill").toString.toInt, g.get("death").toString.toInt, g.get("assist").toString.toInt,
//          g.get("wards").toString.toInt, g.get("cs").toString.toDouble * 60 / g.get("length").toString.toInt)
//      )))
//    }
//
//    //rawRDD.foreach(rdd => gamesRDD.++(rdd))
//
//    gamesRDD.map(
//      g => g.toString
//    ).foreach(println(_))
//
//  }
}
