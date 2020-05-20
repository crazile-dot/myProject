package Query1.utils;

import org.joda.time.DateTime;

public class CsvParser {

    public static Day_Ita parseCSV(String csvLine) {

        Day_Ita day = null;
        String[] csvValues = csvLine.split(",");

        DateTime value1 = DateParser.dateTimeParser(csvValues[0]);
        String value2 = csvValues[1];
        int value3 = Integer.parseInt(csvValues[2]);
        int value4 = Integer.parseInt(csvValues[3]);
        int value5 = Integer.parseInt(csvValues[4]);
        int value6 = Integer.parseInt(csvValues[5]);
        int value7 = Integer.parseInt(csvValues[6]);
        int value8 = Integer.parseInt(csvValues[7]);
        int value9 = Integer.parseInt(csvValues[8]);
        int value10 = Integer.parseInt(csvValues[9]);
        int value11 = Integer.parseInt(csvValues[10]);
        int value12 = Integer.parseInt(csvValues[11]);
        int value13 = Integer.parseInt(csvValues[12]);
        //int value14 = Integer.parseInt(csvValues[13]);
        //String value15 = csvValues[14];
        //String value16 = csvValues[15];

        day = new Day_Ita(
                value1, value2, value3, value4, value5, value6, value7, value8, value9, value10, value11, value12, value13
                // value14,
                // value15,
                //value16
        );

        return day;
    }

}
