package nosql.workshop.batch.elasticsearch.util;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Classe utilitaire pour les jobs ElasticSearch
 */
public abstract class ElasticSearchBatchUtils {
    public static final String ES_DEFAULT_HOST = "localhost";
    public static final int ES_DEFAULT_PORT = 9300;

    private ElasticSearchBatchUtils(){}


    public static DBCursor getMongoCursorToAllInstallations(MongoClient mongoClient) {
        DB db = mongoClient.getDB("nosql-workshop");
        DBCollection installationsCollection = db.getCollection("installations");

        return installationsCollection.find();
    }

    public static String handleComma(String line) {
        Pattern pattern = Pattern.compile("(.*\\d+),(\\d+,\\d+),(\\d+.*)");
        Matcher matcher = pattern.matcher(line);

        if(matcher.matches()){
            line = matcher.group(1) + "." + matcher.group(2) + "." + matcher.group(3);
        }
        return line;
    }

    public static void dealWithFailures(BulkResponse bulkResponse) {
        if (bulkResponse.hasFailures()) {
            System.out.println("Bulk insert has failures : ");
            BulkItemResponse[] items = bulkResponse.getItems();
            for (BulkItemResponse bulkItemResponse : items) {
                System.out.println(bulkItemResponse.getFailure());
            }
        }
    }

    /**
     * Vérifie l'existance de l'index lève une
     * @param indexToChek
     */
    public static void checkIndexExists(String indexToChek, Client elasticSearchClient){

        boolean indexExists = elasticSearchClient
                .admin()
                .indices()
                .exists(new IndicesExistsRequest(indexToChek))
                .actionGet()
                .isExists();
        if(!indexExists){
            throw new UnsupportedOperationException(indexToChek + " does not exist");
        }
    }
}
