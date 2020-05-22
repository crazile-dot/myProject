package Query1;

import Query1.utils.DayIta;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.joda.time.DateTime;
import scala.Tuple2;

public class Average {

    public static JavaPairRDD<DateTime, Double>
        computeHealedDischargedAverage(JavaPairRDD<Tuple2<DayIta, DayIta>, Long> pairRDD, int weekLength) {

        JavaPairRDD<DateTime, Double> healedDischargedRdd = pairRDD.mapToPair(
                new PairFunction<Tuple2<Tuple2<DayIta, DayIta>, Long>, DateTime, Double>() {
                    public Tuple2<DateTime, Double> call(Tuple2<Tuple2<DayIta, DayIta>, Long> t) {
                        DateTime date = t._1()._2().getDate();
                        double a, b;
                        double mean;
                        if (t._2() == 0) {
                            a = 0;
                            b = Double.valueOf(t._1()._2().getHealedDischarged());
                        } else {
                            a = Double.valueOf(t._1()._1().getHealedDischarged());
                            b = Double.valueOf(t._1()._2().getHealedDischarged());
                        }
                        mean = (b - a) / weekLength;
                        Tuple2<DateTime, Double> tuple = new Tuple2<>(date, mean);
                        return tuple;
                    }
                }
        );
        return healedDischargedRdd;
    }

    public static JavaPairRDD<DateTime, Double>
        computeSwabsAverage(JavaPairRDD<Tuple2<DayIta, DayIta>, Long> pairRDD, int weekLength) {

        JavaPairRDD<DateTime, Double> swabsRdd = pairRDD.mapToPair(
                new PairFunction<Tuple2<Tuple2<DayIta, DayIta>, Long>, DateTime, Double>() {
                    public Tuple2<DateTime, Double> call(Tuple2<Tuple2<DayIta, DayIta>, Long> t) {
                        DateTime date = t._1()._2().getDate();
                        double a, b;
                        double mean;
                        if (t._2() == 0) {
                            a = 0;
                            b = Double.valueOf(t._1()._2().getSwabs());
                        } else {
                            a = Double.valueOf(t._1()._1().getSwabs());
                            b = Double.valueOf(t._1()._2().getSwabs());
                        }
                        mean = (b - a) / weekLength;
                        Tuple2<DateTime, Double> tuple = new Tuple2<>(date, mean);
                        return tuple;
                    }
                }
        );
        return swabsRdd;
    }
}
