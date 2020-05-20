package Query1;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;
import scala.Tuple3;
import Query1.utils.CsvDimension;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

public class Split {

    public static ArrayList<JavaPairRDD<Tuple3<DateTime, Integer, Integer>, Long>>
        splitData(List<Tuple3<DateTime, Integer, Integer>> weekRdd, String pathToFile,
                  int weekLength, JavaSparkContext sc) {

        ArrayList<JavaPairRDD<Tuple3<DateTime, Integer, Integer>, Long>> myRdds = new ArrayList<>();
        try {
            int rowsNum = CsvDimension.countNumRows(pathToFile);
            for (int i = 1; i <= rowsNum/weekLength; i++) {
                ArrayList<Tuple3<DateTime, Integer, Integer>> res = new ArrayList<>();
                for (int j = 0; j < weekLength; j++) {
                    Tuple3<DateTime, Integer, Integer> temp = weekRdd.get(((i-1)*weekLength)+j);
                    res.add(temp);
                }
                JavaPairRDD<Tuple3<DateTime, Integer, Integer>, Long> loopRdd = sc.parallelize(res).zipWithIndex();
                myRdds.add(loopRdd);
            }

        } catch (
                FileNotFoundException nfe) {
            System.out.println("Errore del file");
        }
        return myRdds;
    }
}
