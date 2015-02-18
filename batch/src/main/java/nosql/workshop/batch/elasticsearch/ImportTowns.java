package nosql.workshop.batch.elasticsearch;

import static nosql.workshop.batch.elasticsearch.util.ElasticSearchBatchUtils.dealWithFailures;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import nosql.workshop.batch.elasticsearch.util.ElasticSearchBatchUtils;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 * Job d'import des rues de towns_paysdeloire.csv vers ElasticSearch
 * (/towns/town)
 */
public class ImportTowns {
	public static void main(String[] args) throws IOException {
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(
				ImportTowns.class.getResourceAsStream("/csv/towns_paysdeloire.csv")));
				Client elasticSearchClient = new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", "TheBestTocard")
						.build()).addTransportAddress(new InetSocketTransportAddress("localhost", 9300));) {

			// checkIndexExists("towns", elasticSearchClient);

			BulkRequestBuilder bulkRequest = elasticSearchClient.prepareBulk();

			reader.lines().skip(1).filter(line -> line.length() > 0).forEach(line -> insertTown(line, bulkRequest, elasticSearchClient));

			BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();

			dealWithFailures(bulkItemResponses);
		}

	}

	private static void insertTown(String line, BulkRequestBuilder bulkRequest, Client elasticSearchClient) {
		line = ElasticSearchBatchUtils.handleComma(line);

		String[] split = line.split(",");

		String townName = split[1].replaceAll("\"", "");
		Double longitude = Double.valueOf(split[6]);
		Double latitude = Double.valueOf(split[7]);
		Double[] location = { longitude, latitude };


		Map<String, Object> globalMap = new HashMap<String, Object>();
		globalMap.put("townName", townName);
		globalMap.put("location", location);

		Map<String, Object> townNameSuggestContent = new HashMap<String, Object>();
		townNameSuggestContent.put("input", townName);
		townNameSuggestContent.put("output", townName);
		
		Map<String, Object> playLoadContent = new HashMap<String, Object>();
		playLoadContent.put("townName", townName);
		playLoadContent.put("location", location);
		townNameSuggestContent.put("payload", playLoadContent);
		globalMap.put("townNameSuggest", townNameSuggestContent);
		
		bulkRequest.add(elasticSearchClient.prepareIndex("towns", "town").setSource(globalMap));
	}
}
