package Query2.util;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.execution.datasources.csv.CSVUtils;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

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
        checkMonotonicity(intValues);
        state = new State(value1, value2, value3, value4, value5, intValues, 0.0);

        return state;
    }

    public static void checkMonotonicity(ArrayList<Integer> arrayList) {
        for(int i = 1; i < arrayList.size(); i++) {
            if(arrayList.get(i-1) > arrayList.get(i)) {
                if(arrayList.indexOf(arrayList.get(i-1)) == 0) {
                    arrayList.set(0, arrayList.get(i));
                } else {
                    arrayList.set(arrayList.indexOf(arrayList.get(i-1)), (arrayList.get(i-2) + arrayList.get(i))/2);
                }
                checkMonotonicity(arrayList);
            }
        }
    }

    public static ArrayList<String> getDates(String csvLine) {
        ArrayList<String> res = new ArrayList<>();
        String[] csvValues = csvLine.split(",");

        for(int i = 5; i < csvValues.length; i++) {
            res.add(csvValues[i]);
        }
        return res;
    }

}
