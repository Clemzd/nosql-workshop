package nosql.workshop.batch.mongodb;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
public class EquipementsImporter {
	private final DBCollection installationsCollection;
	public EquipementsImporter(DBCollection installationsCollection) {
		this.installationsCollection = installationsCollection;
	}
	public void run() {
		InputStream is = CsvToMongoDb.class.getResourceAsStream("/csv/equipements.csv");
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
			reader.lines().skip(1).filter(line -> line.length() > 0).forEach(line -> updateInstallation(line));
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
	private void updateInstallation(final String line) {
		String[] columns = line.split(",");
		String installationId = columns[2];
		// select the good installation
		DBObject selectionCriteria = new BasicDBObject("_id", installationId);
		
		// the new equipement to add
		DBObject newEquipement = new BasicDBObject()
		.append("numero", columns[4])
		.append("nom", columns[5])
		.append("type", columns[7])
		.append("famille", columns[9]);
		
		// modification
		DBObject modifications = new BasicDBObject();
		modifications.put("$push", new BasicDBObject("equipements", newEquipement));
		
		installationsCollection.update(selectionCriteria, modifications);
	}
}