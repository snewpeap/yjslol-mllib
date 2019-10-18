import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import scala.Tuple9;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MongoDBReceiver {
    private static List<Tuple9<String, Integer, String, String, Integer, Integer, Integer, Integer, Double>> games = new ArrayList<>();

    static List<Tuple9<String, Integer, String, String, Integer, Integer, Integer, Integer, Double>> getGames() {
        AggregateIterable<Document> iterable = MongoClients.create().getDatabase("lol").getCollection("games")
                .aggregate(
                        Arrays.asList(
                                new Document("$match",
                                        new Document("updateAt",
                                                new Document("$eq", 1570936560L))
                                                .append("games",
                                                        new Document("$exists", true))
                                                .append("rank",
                                                        new Document("$lte", 300))),
                                new Document("$project",
                                        new Document("_id", 0L)
                                                .append("sid", 1L)
                                                .append("sname", 1L)
                                                .append("rank", 1L)
                                                .append("gs",
                                                        new Document("$filter",
                                                                new Document("input", "$games")
                                                                        .append("as", "g")
                                                                        .append("cond",
                                                                                new Document()
                                                                        )
                                                        ))
                                )
                        )
                );
        Iterator<Document> iterator = iterable.iterator();
        Document doc;
        while (iterator.hasNext()) {
            doc = iterator.next();
            String sid = doc.getString("sid");
            Integer rank = doc.getInteger("rank");
            String sname = doc.getString("sname");
            List<Document> gs = doc.getList("gs", Document.class);
            for (Document d : gs) {
                games.add(new Tuple9<>(
                        sid, rank, sname, d.getString("gid"), d.getInteger("kill"),
                        d.getInteger("death"), d.getInteger("assist"),
                        d.getInteger("wards"),
                        new BigDecimal(d.getInteger("cs") * 60).divide(new BigDecimal(d.getInteger("length")), 2, RoundingMode.HALF_UP).doubleValue())
                );
            }
        }

        return games;
    }
}
