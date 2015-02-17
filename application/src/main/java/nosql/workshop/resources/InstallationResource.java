package nosql.workshop.resources;

import com.google.inject.Inject;
import net.codestory.http.Context;
import net.codestory.http.Query;
import net.codestory.http.Response;
import net.codestory.http.annotations.Get;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.InstallationsStats;
import nosql.workshop.services.InstallationService;
import nosql.workshop.services.SearchService;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static net.codestory.http.errors.NotFoundException.notFoundIfNull;

/**
 * Resource permettant de gérer l'accès à l'API pour les Installations.
 */
public class InstallationResource {

    public static final int DEFAULT_PAGE = 1;
    public static final int DEFAULT_PAGE_SIZE = 10;
    public static final int MAX_PAGE_SIZE = 50;

    private final InstallationService installationService;
    private final SearchService searchService;

    @Inject
    public InstallationResource(InstallationService installationService, SearchService searchService) {
        this.installationService = installationService;
        this.searchService = searchService;
    }

    @Get("/")
    public List<Installation> list(Context context) {
        // Nombre total d'installations
        long count = installationService.count();

        // Gestion des query params
        Query query = context.query();
        int pageQuery = query.getInteger("page");
        int pageSizeQuery = query.getInteger("pageSize");
        int page = (pageQuery > 0) ? pageQuery : DEFAULT_PAGE;
        int pageSize = (pageSizeQuery > 0 && pageSizeQuery <= MAX_PAGE_SIZE) ? pageSizeQuery : DEFAULT_PAGE_SIZE;

        // Gestion des headers de la réponse
        Response response = context.response();
        List<String> links = new ArrayList<>();
        if (page > 1) {
            links.add("<" + context.request().uri() + "?page=" + (page - 1) + "&pageSize=" + pageSize + ">; rel=\"prev\"");
        }
        if (page * pageSize < count) {
            links.add("<" + context.request().uri() + "?page=" + (page + 1) + "&pageSize=" + pageSize + ">; rel=\"next\"");
        }
        if (!links.isEmpty()) {
            response.setHeader("Link", links.stream().collect(Collectors.joining(", ")));
        }

        return this.installationService.list(page, pageSize);
    }

    @Get("/:numero")
    public Installation get(String numero) {
        return notFoundIfNull(this.installationService.get(numero));
    }


    @Get("/random")
    public Installation random() {
        return installationService.random();
    }

    @Get("/search")
    public List<Installation> search(Context context) {
        String searchQuery = context.query().get("query");
        return installationService.search(searchQuery);
    }

    @Get("/geosearch")
    public List<Installation> geosearch(Context context) {
        Query query = context.query();
        double lat = query.getDouble("lat");
        double lng = query.getDouble("lng");
        double distance = query.getDouble("distance");
        return installationService.geosearch(lat, lng, distance);
    }

    @Get("/stats")
    public InstallationsStats stats() {
        InstallationsStats stats = new InstallationsStats();
        stats.setTotalCount(installationService.count());
        stats.setCountByActivity(installationService.countByActivity());
        stats.setInstallationWithMaxEquipments(installationService.installationWithMaxEquipments());
        stats.setAverageEquipmentsPerInstallation(installationService.averageEquipmentsPerInstallation());
        return stats;
    }
}
