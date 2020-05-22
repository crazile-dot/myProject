package Query2;

import Query2.util.Query2CsvParser;
import Query2.util.State;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;

public class Query2Preprocessing {

    public static JavaRDD<State> preprocessing(JavaRDD<String> rdd) {

        //Tolgo la prima riga del csv cioè i nomi delle colonne
        JavaPairRDD<String, Long> rddWithIndex = rdd.zipWithIndex();
        JavaRDD<String> withoutFirstRow = rddWithIndex.filter((Tuple2<String, Long> t) ->
                t._2() > 0).map(t -> t._1());

        //Parse del csv in oggetti State
        JavaRDD<State> parseRdd = withoutFirstRow.map(line -> Query2CsvParser.parseCSV(line)).filter(x -> x != null);

        /*Prendo solo i valori maggiori di zero per calcolare il trendline coefficient.
        L'inclinazione della retta si ha a partire dal primo contagio verificatosi nel rispettivo stato.
         */
        JavaRDD<State> nonZeroValues = parseRdd.map(new Function<State,State>(){
            public State call(State state) {
                State temp = new State(state.getContinent(), state.getState(), state.getCountry(),
                        state.getLat(), state.getLon(), null, state.getCoefficient());
                ArrayList<Integer> array = new ArrayList<>();
                for(Integer elem : state.getValues()) {
                    if (elem > 0) {
                        array.add(elem);
                    }
                }
                temp.setValues(array);
                return temp;
            }
        }).filter(s -> !s.getValues().isEmpty());

        /*List<State> list = first100.collect();
        for(State elem : list) {
            System.out.println(elem.getValues());
            System.out.println(elem.getCoefficient());
        }*/

        return nonZeroValues;
    }

}
