import Query1.Average;
import Query1.Query1Preprocessing;
import Query1.util.DayIta;
import Query1.util.Query1CsvParser;
import Query1.util.Query1CsvWriter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import scala.Tuple2;

import java.io.IOException;

public class Query1Main {

    private final static int weekLength = 7;
    //s3://mysabdbucketemraws
    private final static String pathToFile = "s3://mysabdbucketemraws/dpc-covid19-ita-andamento-nazionale.csv";
    private final static String outputFile = "src/main/java/Results/query1_output.csv";

    public static void main (String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Query 1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        long start = System.currentTimeMillis();

        JavaRDD<String> covid19File = sc.textFile(pathToFile);
        JavaPairRDD<Tuple2<DayIta, DayIta>, Long> rdd = Query1Preprocessing.preprocessing(covid19File, weekLength);
        JavaPairRDD<DateTime, Double> healedDischargedRdd = Average.computeHealedDischargedAverage(rdd, weekLength);
        JavaPairRDD<DateTime, Double> swabsRdd = Average.computeSwabsAverage(rdd, weekLength);

        try {
            Query1CsvWriter.makeCsv(healedDischargedRdd, swabsRdd, outputFile);
        } catch (IOException io) {
            io.printStackTrace();
            System.out.println("Errore del file");
        }

        sc.close();
        System.out.println("Time query1: " + (System.currentTimeMillis() - start) + "ms");

    }
}


