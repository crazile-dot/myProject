package Query2.util;

import java.util.ArrayList;

public class Continent {

    private String continent;
    private ArrayList<Integer> values;

    public Continent(String continent, ArrayList<Integer> values) {
        this.continent = continent;
        this.values = values;
    }

    public String getContinent() {
        return continent;
    }

    public ArrayList<Integer> getValues() {
        return values;
    }
}
