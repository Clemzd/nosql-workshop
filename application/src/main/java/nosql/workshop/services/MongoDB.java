package nosql.workshop.services;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import org.jongo.Jongo;

import java.net.UnknownHostException;

/**
 * Classe utilitaire permettant de fournir une connexion à la base MongoDB.
 */
public class MongoDB {

    public static final String DB_NAME = "nosql-workshop";

    /**
     * Retourne une instance Jongo permettant de se connecter à la base MongoDB.
     *
     * @return l'instance Jongo.
     * @throws UnknownHostException si la base n'est pas disponible.
     */
    public Jongo getJongo() throws UnknownHostException {
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB(DB_NAME);
        return new Jongo(db);
    }
}
