package Query2;

import Query2.util.Continent;
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
import java.util.List;

public class Query2Main {

    private final static String pathToFile = "data/time_series_covid19_confirmed_global.csv";

    public static void main (String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 2");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        JavaRDD<String> globalCovid19File = sc.textFile(pathToFile).cache();
        JavaRDD<State> parsedRdd = Query2Preprocessing.preprocessing(globalCovid19File);
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

        //
        JavaPairRDD<String, Long> rddWithIndex = globalCovid19File.zipWithIndex();
        JavaRDD<String> firstRow = rddWithIndex.filter((Tuple2<String, Long> t) ->
                t._2() < 1).map(t -> t._1()).filter(s -> !"Continent".substring(s) && !s.equals("Province/State")
        && !s.equals());




    }

}
