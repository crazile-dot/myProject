package Query2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Query2Main {

    private final static String pathToFile = "data/time_series_covid19_confirmed_global.csv";

    public static void main (String[] args) {

        long start = System.currentTimeMillis();

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 2");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        JavaRDD<String> globalCovid19File = sc.textFile(pathToFile);




    }


}
