package Query1;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;
import scala.Tuple2;
import scala.Tuple3;
import Query1.utils.CsvParser;
import Query1.utils.Day_Ita;
import Query1.utils.TupleGenerator;

import java.util.ArrayList;
import java.util.List;

public class Query1Preprocessing {

    public static List<Tuple3<DateTime, Integer, Integer>> preprocessing(JavaRDD<String> origin, JavaSparkContext sc) {
        //public static JavaPairRDD<Tuple3<DateTime, Integer, Integer>, Long> preprocessing(JavaRDD<String> origin, JavaSparkContext sc) {

        List<String> notAllowedLine = new ArrayList<>();
        notAllowedLine.add(origin.first());
        JavaRDD<String> notAllowedRdd = sc.parallelize(notAllowedLine);
        JavaRDD<String> withoutFirstRow = origin.subtract(notAllowedRdd);

        JavaRDD<Day_Ita> values = withoutFirstRow.map(line -> CsvParser.parseCSV(line)).filter(x -> x != null);
        JavaRDD<Tuple3<DateTime, Integer, Integer>> filteredRdd = values.map(elem ->
                TupleGenerator.getTuple3(elem));
        JavaRDD<Tuple3<DateTime, Integer, Integer>> rdd = filteredRdd.sortBy(x -> x._1(), true, 1);
        //JavaPairRDD<Tuple3<DateTime, Integer, Integer>, Long> rdd = filteredRdd.sortBy(x -> x._1(), true, 1).zipWithIndex();
        List<Tuple3<DateTime, Integer, Integer>> weekRdd = rdd.collect();

        return weekRdd;
    }
}
