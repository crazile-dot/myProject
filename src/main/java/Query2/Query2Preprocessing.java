package Query2;

import Query2.util.Query2CsvParser;
import Query2.util.State;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Query2Preprocessing {

    public static JavaRDD<State> preprocessing(JavaRDD<String> rdd) {

        //Tolgo la prima riga del csv cioè i nomi delle colonne
        JavaPairRDD<String, Long> rddWithIndex = rdd.zipWithIndex();
        JavaRDD<String> withoutFirstRow = rddWithIndex.filter((Tuple2<String, Long> t) ->
                t._2() > 0).map(t -> t._1());

        //Parse del csv in oggetti State
        JavaRDD<State> parseRdd = withoutFirstRow.map(line -> Query2CsvParser.parseCSV(line)).filter(x -> x != null);
        /*List<State> list1 = parseRdd.collect();
        System.out.println("Phase1:\n");
        System.out.println("\n" + list1.get(0).getValues().size());*/

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
                s.setValues(arrayList);
                return s;
            }
        });
        /*List<State> list2 = punctualValues.collect();
        System.out.println("Phase2:\n");
        System.out.println("\n" + list2.get(0).getValues().size());
        System.exit(0);*/

        return punctualValues;
    }

    /*Prendo solo i valori maggiori di zero per calcolare il trendline coefficient.
        L'inclinazione della retta si ha a partire dal primo contagio verificatosi nel rispettivo stato.
         */
    public static JavaRDD<State> getNonZeroValues(JavaRDD<State> rdd) {
        JavaRDD<State> nonZeroValues = rdd.map(new Function<State,State>(){
            @Override
            public State call(State state) throws Exception{
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

        return nonZeroValues;
    }

}
