package Query2.util;

import java.io.Serializable;
import java.util.ArrayList;

public class State implements Serializable {

    private String continent;
    private String state;
    private String country;
    private double lat;
    private double lon;
    private ArrayList<Integer> values;
    private double coefficient;

    public State(String continent, String state, String country, double lat, double lon, ArrayList<Integer> values, double coefficient) {
        this.continent = continent;
        this.state = state;
        this.country = country;
        this.lat = lat;
        this.lon = lon;
        this.values = values;
        this.coefficient = coefficient;
    }

    public String getContinent() {
        return continent;
    }

    public String getState() {
        return state;
    }

    public String getCountry() {
        return country;
    }

    public double getLat() {
        return lat;
    }

    public double getLon() {
        return lon;
    }

    public ArrayList<Integer> getValues() {
        return values;
    }

    public double getCoefficient() {
        return coefficient;
    }

    public void setValues(ArrayList<Integer> values) {
        this.values = values;
    }

    public void setCoefficient(double coefficient) {
        this.coefficient = coefficient;
    }
}
