package Query2;

import Query2.util.State;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Query2Main {

    private final static String pathToFile = "data/time_series_covid19_confirmed_global.csv";

    public static void main (String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 2");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        JavaRDD<String> globalCovid19File = sc.textFile(pathToFile);
        JavaRDD<State> nonZeroValues = Query2Preprocessing.preprocessing(globalCovid19File);
        JavaRDD<State> first100 = TrendCompute.get100States(nonZeroValues);

    }












}
