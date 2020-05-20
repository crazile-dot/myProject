package Query1;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;

public class Average {

    public static ArrayList<List> computeAverage(
            ArrayList<JavaPairRDD<Tuple3<DateTime, Integer, Integer>, Long>> myRdds,
            int weekLength, JavaSparkContext sc) {

        Double hdDifference = 0.0;
        Double sDifference = 0.0;

        ArrayList<List> myAverageRdds = new ArrayList<>();
        for (JavaPairRDD<Tuple3<DateTime, Integer, Integer>, Long> elem : myRdds) {
            List<Double> difference1 = new ArrayList<>();
            List<Double> difference2 = new ArrayList<>();
            difference1.add(hdDifference);
            difference2.add(sDifference);

            //Valori double da sottrarre a ogni RDD prima di mediare
            JavaRDD<Double> difference1RDD = sc.parallelize(difference1);
            JavaRDD<Double> difference2RDD = sc.parallelize(difference2);

            //Ultimo giorno di ogni settimana
            JavaPairRDD<Tuple3<DateTime, Integer, Integer>, Long> weekRdd = elem.filter(
                    (Tuple2<Tuple3<DateTime, Integer, Integer>, Long> t) -> t._2() >= weekLength - 1);
            //Primo giorno di ogni settimana perché voglio la data di inizio settimana
            JavaPairRDD<Tuple3<DateTime, Integer, Integer>, Long> weekDayRdd = elem.filter(
                    (Tuple2<Tuple3<DateTime, Integer, Integer>, Long> t) -> t._2() == 0);

            //Calcolo media dei dimessi guariti
            JavaRDD<Double> healedDischargedRDD = weekRdd.map((Tuple2<Tuple3<DateTime, Integer, Integer>, Long> t)
                    -> t._1()._2().doubleValue()).union(difference1RDD);
            Double healedDischargedAverage = healedDischargedRDD.reduce((a,b) -> (a-b)/weekLength);
            List<Double> temp2 = new ArrayList<>();
            temp2.add(healedDischargedAverage);
            JavaRDD<Double>  healedDischargedAverageRDD = sc.parallelize(temp2);

            //Calcolo media dei tamponi
            JavaRDD<Double> swabsRDD = weekRdd.map((Tuple2<Tuple3<DateTime, Integer, Integer>, Long> t)
                    -> t._1()._3().doubleValue()).union(difference2RDD);
            Double swabsAverage = swabsRDD.reduce((a,b) -> (a-b)/weekLength);
            List<Double> temp1 = new ArrayList<>();
            temp1.add(swabsAverage);
            JavaRDD<Double>  swabsAverageRDD = sc.parallelize(temp1);

            //Aggiorno i valori che voglio sottrarre alla prossima iterazione
            hdDifference = weekRdd.reduce((t, l) -> t)._1()._2().doubleValue();
            sDifference = weekRdd.reduce((t, l) -> t)._1()._3().doubleValue();

            //Estraggo la data di inizio settimana per il mio output
            JavaRDD<DateTime> days = weekDayRdd.map((Tuple2<Tuple3<DateTime, Integer, Integer>, Long> t)
                    -> t._1()._1());

            //Creo un unico RDD per stampare i valori
            JavaRDD temp = healedDischargedAverageRDD.union(swabsAverageRDD);
            JavaRDD union = days.union(temp);
            List averageList = union.collect();
            myAverageRdds.add(averageList);
        }
        return myAverageRdds;
    }
}
