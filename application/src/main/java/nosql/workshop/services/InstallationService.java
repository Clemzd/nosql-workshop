package nosql.workshop.services;

import com.google.inject.Inject;

import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.Average;
import nosql.workshop.model.stats.CountByActivity;

import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Service permettant de manipuler les installations sportives.
 */
public class InstallationService {

	/**
	 * Nom de la collection MongoDB.
	 */
	public static final String COLLECTION_NAME = "installations";

	private final MongoCollection installations;

	@Inject
	public InstallationService(MongoDB mongoDB) throws UnknownHostException {
		this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
	}

	/**
	 * Retourne une installation étant donné son numéro.
	 *
	 * @param numero
	 *            le numéro de l'installation.
	 * @return l'installation correspondante, ou <code>null</code> si non
	 *         trouvée.
	 */
	public Installation get(String numero) {
		// TODO codez le service
		return this.installations.findOne("{_id:#}", numero).as(Installation.class);
	}

	/**
	 * Retourne la liste des installations.
	 *
	 * @param page
	 *            la page à retourner.
	 * @param pageSize
	 *            le nombre d'installations par page.
	 * @return la liste des installations.
	 */
	public List<Installation> list(int page, int pageSize) {
		// TODO codez le service
		List<Installation> listInstall = new ArrayList<Installation>();
		MongoCursor<Installation> all = this.installations.find()
											.skip((page-1)*pageSize)
											.limit(pageSize)
											.as(Installation.class);
		
		while (all.hasNext()) {
				listInstall.add(all.next());
		}
		
		return listInstall;
	}

	/**
	 * Retourne une installation aléatoirement.
	 *
	 * @return une installation.
	 */
	public Installation random() {
		long count = count();
		int random = new Random().nextInt((int) count);

		// TODO codez le service
		return this.installations.find().skip(random).as(Installation.class).next();
	}

	/**
	 * Retourne le nombre total d'installations.
	 *
	 * @return le nombre total d'installations
	 */
	public long count() {
		return installations.count();
	}

	/**
	 * Retourne l'installation avec le plus d'équipements.
	 *
	 * @return l'installation avec le plus d'équipements.
	 */
	public Installation installationWithMaxEquipments() {
		Installation installation = this.installations
				.aggregate("{$project:{name:1, adresse:1, equipements:1, location:1, multiCommune:1,"
						+ "nbPlacesParking:1, nbPplacesParkingHandicapes:1, nbEqu:{$size:'$equipements'}}}")
				.and("{$sort:{nbEqu:-1}}")
				.and("{$limit:1}")
				.as(Installation.class).get(0);		
		
		return installation;
	}

	/**
	 * Compte le nombre d'installations par activité.
	 *
	 * @return le nombre d'installations par activité.
	 */
	public List<CountByActivity> countByActivity() {
		// TODO codez le service
		return Arrays.asList(new CountByActivity());
	}

	public double averageEquipmentsPerInstallation() {
		// TODO codez le service
		double avg = 0.00;
		
		avg = this.installations.aggregate("{$group: { _id: null, average: { $avg : {$size:'$equipements'} } } }")
										.and("{ $project: {_id:0, average:1} }")
										.as(Average.class)
										.get(0)
										.getAverage();
		
		return avg;
	}

	/**
	 * Recherche des installations sportives.
	 *
	 * @param searchQuery
	 *            la requête de recherche.
	 * @return les résultats correspondant à la requête.
	 */
	public List<Installation> search(String searchQuery) {
		List<Installation> listInstall = new ArrayList<Installation>();
		MongoCursor<Installation> search = this.installations.find("{$text: {$search:#, $language: 'french'}}", searchQuery)
				.projection("{score: {$meta: 'textScore'}}")
				.sort("{score: {$meta: 'textScore'}}")
				.limit(10).as(Installation.class);
		while (search.hasNext()) {
			listInstall.add(search.next());
		}

		return listInstall;
	}

	/**
	 * Recherche des installations sportives par proximité géographique.
	 *
	 * @param lat
	 *            latitude du point de départ.
	 * @param lng
	 *            longitude du point de départ.
	 * @param distance
	 *            rayon de recherche.
	 * @return les installations dans la zone géographique demandée.
	 */
	public List<Installation> geosearch(double lat, double lng, double distance) {
		List<Installation> list = new ArrayList<Installation>();
		this.installations.ensureIndex("{location: '2dsphere'}");
		MongoCursor<Installation> cursor = this.installations.find("{location: {$near: {$geometry: {type: 'Point', coordinates : [#, #]}, $maxDistance: #}}}}", lng, lat, distance).as(Installation.class);
		while (cursor.hasNext()) {
			list.add(cursor.next());
		}

		return list;
	}
}
