package Query2.util;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Collections;

public class Statistics {

    public static JavaPairRDD<String, ArrayList<Tuple2<String, Double>>> computeAverage(JavaPairRDD<String, ArrayList<Tuple2<String, Integer>>> dailyValues, int weekLength) {
        //Calcolo la media per ogni settimana, indicata dalla data del lunedì
        JavaPairRDD<String, ArrayList<Tuple2<String, Double>>> meanRdd = dailyValues.mapToPair(
                new PairFunction<Tuple2<String, ArrayList<Tuple2<String, Integer>>>, String, ArrayList<Tuple2<String, Double>>>() {
                    @Override
                    public Tuple2<String, ArrayList<Tuple2<String, Double>>> call(Tuple2<String, ArrayList<Tuple2<String, Integer>>> t) throws Exception {
                        ArrayList<Tuple2<String, Double>> temp = new ArrayList<>();
                        for (int i = 1; i <= t._2().size() / weekLength; i++) {
                            double sum = 0;
                            for (int j = 0; j < weekLength; j++) {
                                sum += Double.valueOf(t._2().get((i - 1) * weekLength + j)._2());
                            }
                            double mean = sum / weekLength;
                            Tuple2<String, Double> tuple2 = new Tuple2<>(t._2().get((i - 1) * weekLength)._1(), mean);
                            temp.add(tuple2);
                        }
                        Tuple2<String, ArrayList<Tuple2<String, Double>>> res = new Tuple2<>(t._1(), temp);
                        return res;
                    }
                }
        );
        return meanRdd;
    }

    public static JavaPairRDD<String, ArrayList<Tuple2<String, Double>>> computeStandardDeviation
            (JavaPairRDD<String, ArrayList<Tuple2<String, Integer>>> dailyValues, JavaPairRDD<String, ArrayList<Tuple2<String, Double>>> meanRdd, int weekLength) {
        //Calcolo la deviazione standard per ogni settimana
        JavaPairRDD<String, ArrayList<Tuple2<String, Double>>> standardDeviationRdd = dailyValues.join(meanRdd).
                mapToPair(
                        new PairFunction<Tuple2<String, Tuple2<ArrayList<Tuple2<String, Integer>>, ArrayList<Tuple2<String, Double>>>>, String, ArrayList<Tuple2<String, Double>>>() {
                            @Override
                            public Tuple2<String, ArrayList<Tuple2<String, Double>>> call(Tuple2<String, Tuple2<ArrayList<Tuple2<String, Integer>>, ArrayList<Tuple2<String, Double>>>> t) throws Exception {
                                ArrayList<Tuple2<String, Double>> temp = new ArrayList<>();
                                for (int i = 1; i <= t._2()._1().size() / weekLength; i++) {
                                    double mean = t._2()._2().get(i - 1)._2();
                                    double standDev = 0;
                                    for (int j = 0; j < weekLength; j++) {
                                        standDev += Math.pow((Double.valueOf(t._2()._1().get((i - 1) * weekLength + j)._2()) - mean), 2) / weekLength;
                                    }
                                    Tuple2<String, Double> tuple2 = new Tuple2<>(t._2()._1().get((i - 1) * weekLength)._1(), standDev);
                                    temp.add(tuple2);
                                }
                                Tuple2<String, ArrayList<Tuple2<String, Double>>> res = new Tuple2<>(t._1(), temp);
                                return res;
                            }
                        }
                );
        return standardDeviationRdd;
    }

    public static JavaPairRDD<String, ArrayList<Tuple3<String, Integer, Integer>>> computeMinMax
            (JavaPairRDD<String, ArrayList<Tuple2<String, Integer>>> dailyValues, int weekLength) {
        //Calcolo minimo e massimo per ogni settimana
        JavaPairRDD<String, ArrayList<Tuple3<String, Integer, Integer>>> minmaxRdd = dailyValues.mapToPair(
                new PairFunction<Tuple2<String, ArrayList<Tuple2<String, Integer>>>, String, ArrayList<Tuple3<String, Integer, Integer>>>() {
                    @Override
                    public Tuple2<String, ArrayList<Tuple3<String, Integer, Integer>>> call(Tuple2<String, ArrayList<Tuple2<String, Integer>>> t) throws Exception {
                        ArrayList<Tuple3<String, Integer, Integer>> temp = new ArrayList<>();
                        for (int i = 1; i <= t._2().size() / weekLength; i++) {
                            ArrayList<Integer> week = new ArrayList<>();
                            for (int j = 0; j < weekLength; j++) {
                                week.add(t._2().get((i - 1) * weekLength + j)._2());
                            }
                            Collections.sort(week);
                            int min = week.get(0);
                            int max = week.get(week.size() - 1);
                            Tuple3<String, Integer, Integer> tuple2 = new Tuple3<>(t._2().get((i - 1) * weekLength)._1(), min, max);
                            temp.add(tuple2);
                        }
                        Tuple2<String, ArrayList<Tuple3<String, Integer, Integer>>> res = new Tuple2<>(t._1(), temp);
                        return res;

                    }


                });
        return minmaxRdd;
    }

    public static JavaRDD<Continent> getValuesByContinent(JavaRDD<State> rdd) {

        //Assegno il continente come chiave
        JavaPairRDD<String, State> continentMap = rdd.mapToPair(
                new PairFunction<State, String, State>() {
                    @Override
                    public Tuple2<String, State> call(State s) {
                        String continent = s.getContinent();
                        Tuple2<String, State> tuple = new Tuple2<>(continent, s);
                        return tuple;
                    }
                }
        );
        //Somma valori per chiave (per continente)
        JavaRDD<Continent> sumAnyContinentValues = continentMap.reduceByKey(
                new Function2<State, State, State>() {
                    @Override
                    public State call(State state1, State state2) throws Exception {
                        int sum;
                        ArrayList<Integer> array = new ArrayList<>();
                        for (int i = 0; i < state1.getValues().size(); i++) {
                            sum = state1.getValues().get(i) + state2.getValues().get(i);
                            array.add(sum);
                        }
                        State state = new State(state1.getContinent(), null, null, 0.0, 0.0, array, 0.0);
                        return state;
                    }
                }
        ).map(new Function<Tuple2<String, State>, Continent>() {
            @Override
            public Continent call(Tuple2<String, State> t) throws Exception {
                Continent continent = new Continent(t._1(), t._2().getValues());
                return continent;
            }
        });
        return sumAnyContinentValues;
    }

    public static JavaPairRDD<String, ArrayList<Tuple2<String, Integer>>> getValuesWithDate(JavaRDD<State> rdd, JavaPairRDD<String, Long> rddWithIndex) {
        /*prendo le date dalla prima riga del csv e faccio il prodotto cartesiano con il mio rdd
        per associare il giorno ad ogni valore giornaliero
         */
        JavaRDD<Continent> continentRdd = getValuesByContinent(rdd);
        JavaRDD<ArrayList<String>> datesRdd = rddWithIndex.filter((Tuple2<String, Long> t) ->
                t._2() < 1).map(t -> t._1()).map(s -> Query2CsvParser.getDates(s));
        JavaPairRDD<ArrayList<String>, Continent> temp = datesRdd.cartesian(continentRdd);
        JavaPairRDD<String, ArrayList<Tuple2<String, Integer>>> dailyValues = temp.mapToPair(
                new PairFunction<Tuple2<ArrayList<String>, Continent>, String, ArrayList<Tuple2<String, Integer>>>() {
                    @Override
                    public Tuple2<String, ArrayList<Tuple2<String, Integer>>> call(Tuple2<ArrayList<String>, Continent> t) throws Exception {
                        String continent = t._2().getContinent();
                        ArrayList<Tuple2<String, Integer>> arrayList = new ArrayList<>();
                        for (int i = 0; i < t._1().size(); i++) {
                            Tuple2<String, Integer> temp = new Tuple2<>(t._1().get(i), t._2().getValues().get(i));
                            arrayList.add(temp);
                        }
                        Tuple2<String, ArrayList<Tuple2<String, Integer>>> tuple2 = new Tuple2<>(continent, arrayList);

                        return tuple2;
                    }
                }
        );
        return dailyValues;
    }
}
