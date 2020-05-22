package Query1;

import Query1.utils.DayIta;
import Query1.utils.Query1CsvParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;
import scala.Tuple2;

public class Query1Main {

    private final static int weekLength = 7;
    private final static String pathToFile = "data/dpc-covid19-ita-andamento-nazionale.csv";
    private final static String outputFile = "src/main/java/Query1/Results/query1_output.csv";

    public static void main (String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        long start = System.currentTimeMillis();

        JavaRDD<String> covid19File = sc.textFile(pathToFile);
        JavaPairRDD<Tuple2<DayIta, DayIta>, Long> rdd = Query1Preprocessing.preprocessing(covid19File, weekLength);
        JavaPairRDD<DateTime, Double> healedDischargedRdd = Average.computeHealedDischargedAverage(rdd, weekLength);
        JavaPairRDD<DateTime, Double> swabsRdd = Average.computeSwabsAverage(rdd, weekLength);

        Query1CsvParser.makeCsv(healedDischargedRdd, swabsRdd, outputFile);

        sc.close();
        System.out.println("Time query1: " + (System.currentTimeMillis() - start) + "ms");

    }
}


