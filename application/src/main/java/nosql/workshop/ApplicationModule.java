package nosql.workshop;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import nosql.workshop.services.InstallationService;
import nosql.workshop.services.MongoDB;
import nosql.workshop.services.SearchService;
import static nosql.workshop.services.SearchService.*;


/**
 * Module Guice permettant de définir les classes pouvant être injectées.
 */
public class ApplicationModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(MongoDB.class);
        bind(InstallationService.class);
        bind(SearchService.class);
        bindConstant().annotatedWith(Names.named(ES_HOST)).to("localhost");
        bindConstant().annotatedWith(Names.named(ES_TRANSPORT_PORT)).to(9300);
    }
}
