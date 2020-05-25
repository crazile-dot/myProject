package Query2;

import Query2.util.Query2CsvParser;
import Query2.util.State;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;

public class Query2Preprocessing {

    public static JavaRDD<State> preprocessing(JavaPairRDD<String, Long> rdd) {

        //Tolgo la prima riga del csv cioè i nomi delle colonne
        JavaRDD<String> withoutFirstRow = rdd.filter((Tuple2<String, Long> t) ->
                t._2() > 0).map(t -> t._1());

        //Parse del csv in oggetti State
        JavaRDD<State> parseRdd = withoutFirstRow.map(line -> Query2CsvParser.parseCSV(line)).filter(x -> x != null);

        //Trasformo i valori in puntuali
        JavaRDD<State> punctualValues = parseRdd.map(new Function<State,State>(){
            @Override
            public State call(State s) throws Exception{
                ArrayList<Integer> arrayList = new ArrayList<>();
                int punctual;
                arrayList.add(s.getValues().get(0));
                for (int i = 1; i < s.getValues().size(); i++) {
                    punctual = s.getValues().get(i) - s.getValues().get(i-1);
                    arrayList.add(punctual);
                }
                State state = new State(s.getContinent(), s.getState(), s.getCountry(), s.getLat(), s.getLon(), arrayList, s.getCoefficient());
                return state;
            }
        });

        return punctualValues;
    }

}
