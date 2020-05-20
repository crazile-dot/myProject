package Query1;

import org.apache.spark.api.java.JavaPairRDD;
import org.joda.time.DateTime;
import scala.Tuple2;
import scala.Tuple3;

public class NotSplit {

    public static JavaPairRDD<Tuple3<DateTime, Integer, Integer>, Long>
        getWeekValues(JavaPairRDD<Tuple3<DateTime, Integer, Integer>, Long> pairRdd, int weekLength) {

        JavaPairRDD<Tuple3<DateTime, Integer, Integer>, Long> weekRdd = pairRdd.filter(
                (Tuple2<Tuple3<DateTime, Integer, Integer>, Long> t) -> t._2()%weekLength == weekLength-1);

        return weekRdd;
    }
}
