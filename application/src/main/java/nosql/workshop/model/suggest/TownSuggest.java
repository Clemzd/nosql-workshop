package nosql.workshop.model.suggest;

import java.util.List;

/**
 * Created by Chris on 13/02/15.
 */
public class TownSuggest {
    private String townName;
    private Double[] location;

    private TownSuggest() {
    }

    public TownSuggest(String value, List<Double> location) {
        this.townName = value;
        this.location = location.toArray(new Double[location.size()]);
    }

    public String getTownName() {
        return townName;
    }

    public void setTownName(String townName) {
        this.townName = townName;
    }

    public Double[] getLocation() {
        return location;
    }

    public void setLocation(Double[] location) {
        this.location = location;
    }
}
