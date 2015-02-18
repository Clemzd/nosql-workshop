package nosql.workshop.services;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import nosql.workshop.model.Installation;
import nosql.workshop.model.suggest.TownSuggest;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 * Created by Chris on 12/02/15.
 */
public class SearchService {
	public static final String INSTALLATIONS_INDEX = "installations";
	public static final String INSTALLATION_TYPE = "installation";
	public static final String TOWNS_INDEX = "towns";
	private static final String TOWN_TYPE = "town";

	public static final String ES_HOST = "es.host";
	public static final String ES_TRANSPORT_PORT = "es.transport.port";

	final Client elasticSearchClient;
	final ObjectMapper objectMapper;

	@Inject
	public SearchService(@Named(ES_HOST) String host, @Named(ES_TRANSPORT_PORT) int transportPort) {
		elasticSearchClient = new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", "TheBestTocard").build())
				.addTransportAddress(new InetSocketTransportAddress(host, transportPort));

		objectMapper = new ObjectMapper();
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	/**
	 * Recherche les installations à l'aide d'une requête full-text
	 * 
	 * @param searchQuery
	 *            la requête
	 * @return la listes de installations
	 */
	public List<Installation> search(String searchQuery) {
		List<Installation> listInstallations = new ArrayList<Installation>();
		
		SearchResponse response = elasticSearchClient.prepareSearch(INSTALLATIONS_INDEX)
				.setTypes(INSTALLATION_TYPE)
				.setQuery(QueryBuilders.queryString(searchQuery))
				.setExplain(true)
				.execute().actionGet();
		
		SearchHit[] iteratorHit = response.getHits().getHits();
		for(int i = 0; i < iteratorHit.length; i++){
			SearchHit searchHit = iteratorHit[i];
			listInstallations.add(mapToInstallation(searchHit));
		}
		
		return listInstallations;
	}

	/**
	 * Transforme un résultat de recherche ES en objet installation.
	 *
	 * @param searchHit
	 *            l'objet ES.
	 * @return l'installation.
	 */
	private Installation mapToInstallation(SearchHit searchHit) {
		try {
			return objectMapper.readValue(searchHit.getSourceAsString(), Installation.class);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public List<TownSuggest> suggestTownName(String townName) {
		List<TownSuggest> listTownSuggest = new ArrayList<TownSuggest>();

		CompletionSuggestionBuilder completionSuggestionBuilder = new CompletionSuggestionBuilder(TOWNS_INDEX);
		completionSuggestionBuilder.text(townName);
		completionSuggestionBuilder.field("townNameSuggest");

		SearchResponse response = elasticSearchClient.prepareSearch(TOWNS_INDEX)
				.setTypes("completion")
				.setQuery(QueryBuilders.matchAllQuery())
				.addSuggestion(completionSuggestionBuilder)
				.execute().actionGet();

		CompletionSuggestion completionSuggestion = response.getSuggest().getSuggestion("towns");
		
		Iterator<CompletionSuggestion.Entry.Option> iterator = completionSuggestion.iterator().next().getOptions().iterator();
        while (iterator.hasNext()){
            try {
            	listTownSuggest.add(objectMapper.readValue(iterator.next().getPayloadAsString(), TownSuggest.class));
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
        }
        
		return listTownSuggest;
	}

	public Double[] getTownLocation(String townName) {
		SearchResponse response = elasticSearchClient.prepareSearch(TOWNS_INDEX).setTypes(TOWN_TYPE)
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(QueryBuilders.matchQuery("townName", townName))
				.addField("location").execute().actionGet();

		SearchHits searchHits = response.getHits();
		// by default we set the location to Carquefou
		Double[] location = new Double[] { -1.49295, 47.29692 };
		if (searchHits.getHits().length > 0) {
			List<Object> listValues = searchHits.getHits()[0].field("location").values();
			location[0] = (Double) listValues.get(0);
			location[1] = (Double) listValues.get(1);
		} else {
			throw new UnsupportedOperationException();
		}

		return location;
	}
}
