package Query2;

import Query2.util.Regression;
import Query2.util.State;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

public class TrendCompute {

    public static JavaRDD<State> computeTrendlineCoefficient(JavaRDD<State> rdd) {
        JavaRDD<State> coefficientsRdd = rdd.map(new Function<State, State>() {
            public State call(State state) {
                State temp = new State(state.getContinent(), state.getState(), state.getCountry(),
                        state.getLat(), state.getLon(), state.getValues(), Regression.getSlope(state.getValues()));
                return temp;
            }
        });

        return coefficientsRdd;
    }

    /*public static JavaRDD<State> computeTrendlineCoefficient(JavaRDD<State> rdd) {
        JavaRDD<State> coefficientsRdd = rdd.map(new Function<State, State>() {
            public State call(State state) {
                double Y1 = state.getValues().get(0);
                double Y2 = state.getValues().get(state.getValues().size() - 1);
                double deltaX = state.getValues().size();
                State temp = new State(state.getContinent(), state.getState(), state.getCountry(),
                        state.getLat(), state.getLon(), state.getValues(), (Y2-Y1)/deltaX);
                return temp;
            }
        });

        return coefficientsRdd;
    }*/


    public static JavaRDD<State> get100States(JavaRDD<State> rdd) {
        //JavaRDD<State> nonZeroValues = Query2Preprocessing.getNonZeroValues(rdd);
        //JavaRDD<State> coefficients = computeTrendlineCoefficient(nonZeroValues);
        JavaPairRDD<State, Long> orderedCoefficients = rdd.sortBy(s -> s.getCoefficient(), false, 1).zipWithIndex();
        JavaRDD<State> first100 = orderedCoefficients.filter(t -> t._2() < 100).map(t -> t._1());

        return first100;

    }
}
