package Query2;

import Query2.util.Query2CsvParser;
import Query2.util.State;
import Query2.util.Statistics;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

public class Query2Main {

    private final static int weekLength = 7;
    private final static String pathToFile = "data/time_series_covid19_confirmed_global.csv";
    private final static String output = "src/main/java/Results/query2_output.csv";
    private final static String output2 = "src/main/java/Results/query2_output2.csv";
    private final static String output3 = "src/main/java/Results/query2_output3.csv";

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 2");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        long start = System.currentTimeMillis();

        JavaRDD<String> globalCovid19File = sc.textFile(pathToFile);
        JavaPairRDD<String, Long> rddWithIndex = globalCovid19File.zipWithIndex().cache();
        JavaRDD<State> parsedRdd = Query2Preprocessing.preprocessing(rddWithIndex);
        JavaRDD<State> coefficients = TrendCompute.computeTrendlineCoefficient(parsedRdd);
        JavaRDD<State> first100 = TrendCompute.get100States(coefficients);
        JavaPairRDD<String, ArrayList<Tuple2<String, Integer>>> valuesByContinent = Statistics.getValuesWithDate(first100, rddWithIndex).cache();

        JavaPairRDD<String, ArrayList<Tuple2<String, Double>>> meanRdd = Statistics.computeAverage(valuesByContinent, weekLength).cache();
        JavaPairRDD<String, ArrayList<Tuple2<String, Double>>> standardDeviationRdd = Statistics.computeStandardDeviation(valuesByContinent, meanRdd, weekLength);
        JavaPairRDD<String, ArrayList<Tuple3<String, Integer, Integer>>> minMaxRdd = Statistics.computeMinMax(valuesByContinent, weekLength);

        Query2CsvParser.makeCsv(meanRdd, standardDeviationRdd, minMaxRdd, output);
        /*meanRdd.saveAsTextFile(output);
        standardDeviationRdd.saveAsTextFile(output2);
        minMaxRdd.saveAsTextFile(output3);*/
        sc.close();
        System.out.println("Time query2: " + (System.currentTimeMillis() - start) + "ms");


    }
}
