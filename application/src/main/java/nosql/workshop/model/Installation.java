package nosql.workshop.model;


import org.jongo.marshall.jackson.oid.Id;

import java.util.Date;
import java.util.List;

/**
 * Installation sportive.
 */
public class Installation {

    @Id
    private String numero;
    private String nom;
    private Adresse adresse;
    private Location location;
    private boolean multiCommune;
    private int nbPlacesParking;
    private int nbPlacesParkingHandicapes;
//    private Date dateMiseAJourFiche;
    private List<Equipement> equipements;

    public String getNumero() {
        return numero;
    }

    public void setNumero(String numero) {
        this.numero = numero;
    }

    public String getNom() {
        return nom;
    }

    public void setNom(String nom) {
        this.nom = nom;
    }

    public Adresse getAdresse() {
        return adresse;
    }

    public void setAdresse(Adresse adresse) {
        this.adresse = adresse;
    }

    public Location getLocation() {
        return location;
    }

    public void setLocation(Location location) {
        this.location = location;
    }

    public boolean isMultiCommune() {
        return multiCommune;
    }

    public void setMultiCommune(boolean multiCommune) {
        this.multiCommune = multiCommune;
    }

    public int getNbPlacesParking() {
        return nbPlacesParking;
    }

    public void setNbPlacesParking(int nbPlacesParking) {
        this.nbPlacesParking = nbPlacesParking;
    }

    public int getNbPlacesParkingHandicapes() {
        return nbPlacesParkingHandicapes;
    }

    public void setNbPlacesParkingHandicapes(int nbPlacesParkingHandicapes) {
        this.nbPlacesParkingHandicapes = nbPlacesParkingHandicapes;
    }

//    public Date getDateMiseAJourFiche() {
//        return dateMiseAJourFiche;
//    }
//
//    public void setDateMiseAJourFiche(Date dateMiseAJourFiche) {
//        this.dateMiseAJourFiche = dateMiseAJourFiche;
//    }

    public List<Equipement> getEquipements() {
        return equipements;
    }

    public void setEquipements(List<Equipement> equipements) {
        this.equipements = equipements;
    }

    public class Adresse {

        private String numero;
        private String voie;
        private String lieuDit;
        private String codePostal;
        private String commune;

        public String getNumero() {
            return numero;
        }

        public void setNumero(String numero) {
            this.numero = numero;
        }

        public String getVoie() {
            return voie;
        }

        public void setVoie(String voie) {
            this.voie = voie;
        }

        public String getLieuDit() {
            return lieuDit;
        }

        public void setLieuDit(String lieuDit) {
            this.lieuDit = lieuDit;
        }

        public String getCodePostal() {
            return codePostal;
        }

        public void setCodePostal(String codePostal) {
            this.codePostal = codePostal;
        }

        public String getCommune() {
            return commune;
        }

        public void setCommune(String commune) {
            this.commune = commune;
        }


    }

    public class Location {

        private String type;
        private double[] coordinates;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public double[] getCoordinates() {
            return coordinates;
        }

        public void setCoordinates(double[] coordinates) {
            this.coordinates = coordinates;
        }
    }

}
