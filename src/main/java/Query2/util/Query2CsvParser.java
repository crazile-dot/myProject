package Query2.util;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.util.ArrayList;
import java.util.List;

public class Query2CsvParser {

    public static State parseCSV(String csvLine) {

        State state = null;
        String[] csvValues = csvLine.split(",");
        ArrayList<Integer> intValues = new ArrayList<>();

        String value1 = csvValues[0];
        String value2 = csvValues[1];
        String value3 = csvValues[2];
        double value4 = Double.parseDouble(csvValues[3]);
        double value5 = Double.parseDouble(csvValues[4]);
        for(int i = 5; i < csvValues.length; i++) {
            intValues.add(Integer.parseInt(csvValues[i]));
        }
        checkMonotonicity(intValues);
        state = new State(value1, value2, value3, value4, value5, intValues, 0.0);

        return state;
    }

    public static void checkMonotonicity(ArrayList<Integer> arrayList) {
        for(int i = 1; i < arrayList.size(); i++) {
            if(arrayList.get(i-1) > arrayList.get(i)) {
                if(arrayList.indexOf(arrayList.get(i-1)) == 0) {
                    arrayList.set(0, arrayList.get(i));
                } else {
                    arrayList.set(arrayList.indexOf(arrayList.get(i-1)), (arrayList.get(i-2) + arrayList.get(i))/2);
                }
                checkMonotonicity(arrayList);
            }
        }
    }

    public static ArrayList<String> getDates(String csvLine) {
        ArrayList<String> res = new ArrayList<>();
        String[] csvValues = csvLine.split(",");

        for(int i = 5; i < csvValues.length; i++) {
            res.add(csvValues[i]);
        }
        return res;
    }

    public static void makeCsv(JavaPairRDD<String, ArrayList<Tuple2<String, Double>>> rdd1, JavaPairRDD<String, ArrayList<Tuple2<String, Double>>> rdd2,
                               JavaPairRDD<String, ArrayList<Tuple3<String, Integer, Integer>>> rdd3, String output) {
        JavaPairRDD<String, Tuple2<ArrayList<Tuple2<String, Double>>, ArrayList<Tuple2<String, Double>>>> temp = rdd1.join(rdd2);
        JavaPairRDD<String, Tuple2<Tuple2<ArrayList<Tuple2<String, Double>>, ArrayList<Tuple2<String, Double>>>, ArrayList<Tuple3<String, Integer, Integer>>>> join =
                temp.join(rdd3);
        //Raggruppo i valori delle statistiche per settimana
        JavaPairRDD<String, ArrayList<Tuple2<String, Tuple4<Double, Double, Integer, Integer>>>> valuesByDay =
                join.mapToPair(new PairFunction<Tuple2<String, Tuple2<Tuple2<ArrayList<Tuple2<String, Double>>, ArrayList<Tuple2<String, Double>>>, ArrayList<Tuple3<String, Integer, Integer>>>>, String, ArrayList<Tuple2<String, Tuple4<Double, Double, Integer, Integer>>>>() {
                    @Override
                    public Tuple2<String, ArrayList<Tuple2<String, Tuple4<Double, Double, Integer, Integer>>>> call
                            (Tuple2<String, Tuple2<Tuple2<ArrayList<Tuple2<String, Double>>, ArrayList<Tuple2<String, Double>>>, ArrayList<Tuple3<String, Integer, Integer>>>> t) throws Exception {
                        String date = "";
                        ArrayList<Tuple2<String, Tuple4<Double, Double, Integer, Integer>>> arrayList = new ArrayList<>();
                        if (t._2()._1()._1().size() == t._2()._1()._2().size() && t._2()._1()._2().size() == t._2()._2().size()) {
                            for (int i = 0; i < t._2()._1()._1().size(); i++) {
                                if (t._2()._1()._1().get(i)._1().equals(t._2()._1()._2().get(i)._1()) && t._2()._1()._2().get(i)._1().equals(t._2()._2().get(i)._1())) {
                                    date = t._2()._1()._1().get(i)._1();
                                    Tuple4<Double, Double, Integer, Integer> tuple4 = new Tuple4<>(t._2()._1()._1().get(i)._2(),
                                            t._2()._1()._2().get(i)._2(), t._2()._2().get(i)._2(), t._2()._2().get(i)._3());
                                    Tuple2<String, Tuple4<Double, Double, Integer, Integer>> tuple2 = new Tuple2<>(date, tuple4);
                                    arrayList.add(tuple2);
                                } else {
                                    System.out.println("La data non coincide\n");
                                }
                            }
                        } else {
                            System.out.println("Gli array non coincidono\n");
                        }
                        Tuple2<String, ArrayList<Tuple2<String, Tuple4<Double, Double, Integer, Integer>>>> tuple2Again =
                                new Tuple2<>(t._1(), arrayList);
                        return tuple2Again;
                    }
                });

        List<Tuple2<String, ArrayList<Tuple2<String, Tuple4<Double, Double, Integer, Integer>>>>> list = valuesByDay.collect();


        //valuesByDay.saveAsTextFile(output);
    }

}
