package Query1.utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.joda.time.DateTime;
import scala.Tuple2;
import util.DateParser;

public class Query1CsvParser {

    public static DayIta parseCSV(String csvLine) {

        DayIta day = null;
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

        day = new DayIta(
                value1, value2, value3, value4, value5, value6, value7,
                value8, value9, value10, value11, value12, value13

        );

        return day;
    }

    public static void makeCsv(JavaPairRDD<DateTime, Double> rdd1, JavaPairRDD<DateTime, Double> rdd2, String output) {
        //aggiungere un controllo (se esiste già il file, ciao)
        JavaPairRDD<DateTime, Tuple2<Double, Double>> joinedRdd = rdd1.join(rdd2).sortByKey();
        joinedRdd.saveAsTextFile(output);
    }

}
