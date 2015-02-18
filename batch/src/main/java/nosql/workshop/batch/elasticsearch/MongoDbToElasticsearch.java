package nosql.workshop.batch.elasticsearch;

import com.mongodb.*;

import nosql.workshop.batch.elasticsearch.util.ElasticSearchBatchUtils;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;

import static nosql.workshop.batch.elasticsearch.util.ElasticSearchBatchUtils.*;

/**
 * Transferts les documents depuis MongoDB vers Elasticsearch.
 */
public class MongoDbToElasticsearch {

	public static void main(String[] args) throws UnknownHostException {

		MongoClient mongoClient = null;

		long startTime = System.currentTimeMillis();
		try (Client elasticSearchClient = new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", "TheBestTocard")
				.build()).addTransportAddress(new InetSocketTransportAddress(ES_DEFAULT_HOST, ES_DEFAULT_PORT));) {

			// checkIndexExists("installations", elasticSearchClient);

			mongoClient = new MongoClient();

			// cursor all database objects from mongo db
			DBCursor cursor = ElasticSearchBatchUtils.getMongoCursorToAllInstallations(mongoClient);

			BulkRequestBuilder bulkRequest = elasticSearchClient.prepareBulk();

			while (cursor.hasNext()) {
				DBObject object = cursor.next();

				String objectId = (String) object.get("_id");
				object.removeField("dateMiseAJourFiche");

				bulkRequest.add(elasticSearchClient.prepareIndex("installations", "installation", objectId)
						.setSource(object.toMap()));

			}
			BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();

			dealWithFailures(bulkItemResponses);

			System.out.println("Inserted all documents in " + (System.currentTimeMillis() - startTime) + " ms");
		} finally {
			if (mongoClient != null) {
				mongoClient.close();
			}
		}

	}

}
