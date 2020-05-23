package Query2;

import Query2.util.Continent;
import Query2.util.Query2CsvParser;
import Query2.util.State;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.awt.image.ImageProducer;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class Query2Main {

    private final static int weekLength = 7;
    private final static String pathToFile = "data/time_series_covid19_confirmed_global.csv";
    private final static String output = "src/main/java/Query1/Results/query2_output.csv";

    public static void main (String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 2");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        JavaRDD<String> globalCovid19File = sc.textFile(pathToFile);
        JavaPairRDD<String, Long> rddWithIndex = globalCovid19File.zipWithIndex().cache();
        JavaRDD<State> parsedRdd = Query2Preprocessing.preprocessing(rddWithIndex);
        JavaRDD<State> coefficients = TrendCompute.computeTrendlineCoefficient(parsedRdd);
        /*List<State> list = coefficients.collect();
        for (State elem : list) {
            System.out.println(elem.getCoefficient());
        }*/

        JavaRDD<State> first100 = TrendCompute.get100States(coefficients);
        JavaPairRDD<String, State> continentMap = first100.mapToPair(
                new PairFunction<State, String, State>() {
                    @Override
                    public Tuple2<String, State> call(State s) {
                        String continent = s.getContinent();
                        Tuple2<String, State> tuple = new Tuple2<>(continent, s);
                        return tuple;
                    }
                }
        );

        JavaRDD<Continent> sumAnyContinentValues = continentMap.reduceByKey(
                new Function2<State, State, State>() {
                    @Override
                    public State call(State state1, State state2) throws Exception {
                        int sum;
                        ArrayList<Integer> array = new ArrayList<>();
                        for(int i = 0; i < state1.getValues().size(); i++) {
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

        //prendo le date dalla prima riga del csv e faccio il prodotto cartesiano con il mio rdd
        JavaRDD<ArrayList<Date>> datesRdd = rddWithIndex.filter((Tuple2<String, Long> t) ->
                t._2() < 1).map(t -> t._1()).map(s -> Query2CsvParser.parseDates(s));
        JavaPairRDD<ArrayList<Date>, Continent> temp = datesRdd.cartesian(sumAnyContinentValues);
        JavaPairRDD<String, ArrayList<Tuple2<Date, Integer>>> dailyValues = temp.mapToPair(
                new PairFunction<Tuple2<ArrayList<Date>, Continent>, String, ArrayList<Tuple2<Date, Integer>>>() {
                    @Override
                    public Tuple2<String, ArrayList<Tuple2<Date, Integer>>> call(Tuple2<ArrayList<Date>, Continent> t) throws Exception{
                        String continent = t._2().getContinent();
                        ArrayList<Tuple2<Date, Integer>> arrayList = new ArrayList<>();
                        for(int i = 0; i <t._1().size(); i++) {
                            Tuple2<Date, Integer> temp = new Tuple2<>(t._1().get(i), t._2().getValues().get(i));
                            arrayList.add(temp);
                        }
                        Tuple2<String, ArrayList<Tuple2<Date, Integer>>> tuple2 = new Tuple2<>(continent, arrayList);

                        return tuple2;
                    }
                }
        ).cache();
        /*List<Tuple2<String, ArrayList<Tuple2<Date, Integer>>>> l = dailyValues.collect();
        for(Tuple2<String, ArrayList<Tuple2<Date, Integer>>> elem : l) {
            System.out.println(elem);
        }*/
        JavaPairRDD<String, ArrayList<Tuple2<Date, Double>>> meanRdd = dailyValues.mapToPair(
                new PairFunction<Tuple2<String, ArrayList<Tuple2<Date, Integer>>>, String, ArrayList<Tuple2<Date, Double>>>() {
                    @Override
                    public Tuple2<String, ArrayList<Tuple2<Date, Double>>> call(Tuple2<String, ArrayList<Tuple2<Date, Integer>>> t) throws Exception {
                        ArrayList<Tuple2<Date, Double>> temp = new ArrayList<>();
                        for(int i = 1; i <= t._2().size()/weekLength; i++) {
                            double sum = 0;
                            for(int j = 0; j < weekLength; j++) {
                                sum += Double.valueOf(t._2().get((i-1)*weekLength + j)._2());
                            }
                            double mean = sum/weekLength;
                            Tuple2<Date, Double> tuple2 = new Tuple2<>(t._2().get((i-1)*weekLength)._1(), mean);
                            temp.add(tuple2);
                        }
                        Tuple2<String, ArrayList<Tuple2<Date, Double>>> res = new Tuple2<>(t._1(), temp);
                        return res;
                    }
                }
        );

        JavaPairRDD<String, ArrayList<Tuple2<Date, Double>>> standardDeviationRdd = dailyValues.mapToPair(
                new PairFunction<Tuple2<String, ArrayList<Tuple2<Date, Integer>>>, String, ArrayList<Tuple2<Date, Double>>>() {
                    @Override
                    public Tuple2<String, ArrayList<Tuple2<Date, Double>>> call(Tuple2<String, ArrayList<Tuple2<Date, Integer>>> t) throws Exception {
                        ArrayList<Tuple2<Date, Double>> temp = new ArrayList<>();
                        for(int i = 1; i <= t._2().size()/weekLength; i++) {
                            double sum = 0;
                            for(int j = 0; j < weekLength; j++) {
                                sum += Double.valueOf(t._2().get((i-1)*weekLength + j)._2());
                            }
                            double mean = sum/weekLength;
                            Tuple2<Date, Double> tuple2 = new Tuple2<>(t._2().get((i-1)*weekLength)._1(), mean);
                            temp.add(tuple2);
                        }
                        Tuple2<String, ArrayList<Tuple2<Date, Double>>> res = new Tuple2<>(t._1(), temp);
                        return res;
                    }
                }
        );
        meanRdd.saveAsTextFile(output);


    }

}
