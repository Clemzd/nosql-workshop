package nosql.workshop.resources;

import com.google.inject.Inject;
import net.codestory.http.annotations.Get;
import nosql.workshop.model.suggest.TownSuggest;
import nosql.workshop.services.SearchService;

import java.util.List;

/**
 * API REST pour les villes
 * Created by Chris on 12/02/15.
 */
public class TownRessource {
    private final SearchService searchService;

    @Inject
    public TownRessource(SearchService searchService) {
        this.searchService = searchService;
    }

    @Get("suggest/:text")
    public List<TownSuggest> suggest(String text) {
        return searchService.suggestTownName(text);
    }

    @Get("location/:townName")
    public Double[] getLocation(String townName){
        return searchService.getTownLocation(townName);
    }
}
