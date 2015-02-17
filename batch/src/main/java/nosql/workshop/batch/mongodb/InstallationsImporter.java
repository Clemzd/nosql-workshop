package nosql.workshop.batch.mongodb;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
/**
 * Importe les 'installations' dans MongoDB.
 */
public class InstallationsImporter {
    private final DBCollection installationsCollection;
    public InstallationsImporter(DBCollection installationsCollection) {
        this.installationsCollection = installationsCollection;
    }
    public void run() {
        InputStream is = CsvToMongoDb.class.getResourceAsStream("/csv/installations.csv");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .forEach(line -> installationsCollection.save(toDbObject(line)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
    private DBObject toDbObject(final String line) {
        String[] columns = line
                .substring(1, line.length() - 1)
                .split("\",\"");
        
        Date date = null;
        if (columns.length > 28) {
        	// TODO refaire (ca arrondit au jour d'avant à 22h, WTF !)
        	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
     		try {
				date = (Date)sdf.parse(columns[28]);
			} catch (ParseException e) {
				throw new RuntimeException("");
			}
        }
        
       
        // TODO créez le document à partir de la ligne CSV
        BasicDBObject doc = new BasicDBObject();
        doc.append("_id", columns[1])
        .append("nom", columns[0])
        .append("adresse", new BasicDBObject()
        					.append("numero", columns[6])
        					.append("voie", columns[7])
        					.append("lieuDit", columns[5])
        					.append("codePostal", columns[4])
        					.append("commune", columns[2])
        )
        .append("location", new BasicDBObject()
        					.append("type", "Point")
        					.append("coordinates", new String[] { columns[9], columns[10] })
        )
        .append("multiCommune", (columns[16].equals("Non")) ? "false" : "true")
        .append("nbPlacesParking", columns[17])
        .append("nbPlacesParkingHandicapes", columns[18])
        .append("dateMiseAJourFiche", (date == null) ? "" : date)
        ;
        
        
        return doc;
    }
}