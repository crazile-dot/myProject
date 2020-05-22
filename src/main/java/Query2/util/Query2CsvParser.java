package Query2.util;

import java.util.ArrayList;

public class Query2CsvParser {

    public static State parseCSV(String csvLine) {

        State state = null;
        String[] csvValues = csvLine.split(",");
        ArrayList<Integer> intValues = new ArrayList<>();

        String value1 = csvValues[0];
        String value2 = csvValues[1];
        String value3 = csvValues[2];
        double value4 = Double.parseDouble(csvValues[3]);
        double value5 = Double.parseDouble(csvValues[4]);
        for(int i = 5; i < csvValues.length; i++) {
            intValues.add(Integer.parseInt(csvValues[i]));
        }
        state = new State(value1, value2, value3, value4, value5, intValues, 0.0);

        return state;
    }

}
