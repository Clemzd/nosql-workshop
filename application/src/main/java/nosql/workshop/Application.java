package nosql.workshop;

import net.codestory.http.WebServer;
import net.codestory.http.injection.GuiceAdapter;
import nosql.workshop.resources.TownRessource;
import nosql.workshop.resources.InstallationResource;

/**
 * Point d'entrée de l'application. Permet de démarrer le serveur web afin d'exposer l'API et les pages HTML.
 */
public class Application {

    public static void main(String[] args) {
        WebServer webServer = new WebServer();
        webServer.configure(routes -> {
                    routes.setIocAdapter(new GuiceAdapter(new ApplicationModule()));
                    routes.add("/api/installations", InstallationResource.class);
                    routes.add("/api/towns", TownRessource.class);
                }
        );
        webServer.start();
    }

}
