package Query1;

import Query1.utils.CsvOutput;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;

public class Query1Main {

    private final static int weekLength = 7;
    private final static String pathToFile = "data/dpc-covid19-ita-andamento-nazionale.csv";
    private final static String outputFile = "src/main/java/Query1/out/query1_output.csv";

    public static void main (String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        long start = System.currentTimeMillis();

        JavaRDD<String> covid19File = sc.textFile(pathToFile);
        List<Tuple3<DateTime, Integer, Integer>> rdd = Query1Preprocessing.preprocessing(covid19File, sc);
        //JavaPairRDD<Tuple3<DateTime, Integer, Integer>, Long> rdd = Query1Preprocessing.preprocessing(covid19File, sc);
        ArrayList<JavaPairRDD<Tuple3<DateTime, Integer, Integer>, Long>> myRdds = Split.splitData(rdd, pathToFile, weekLength, sc);
        //JavaPairRDD<Tuple3<DateTime, Integer, Integer>, Long> weekRdd = NotSplit.getWeekValues(rdd, weekLength);
        ArrayList<List> myAverageRdds = Average.computeAverage(myRdds, weekLength, sc);
        //System.out.println("Rdd size: " + weekRdd.count());
        //List<Tuple2<Tuple3<DateTime, Integer, Integer>, Long>> tryList = weekRdd.collect();
        //for (Tuple2<Tuple3<DateTime, Integer, Integer>, Long> elem : tryList) {
        //    System.out.println(elem);
        //}

        CsvOutput.printOutputOnCsv(outputFile, myAverageRdds);

        sc.close();
        System.out.println("Time query1: " + (System.currentTimeMillis() - start) + "ms");

    }
}


