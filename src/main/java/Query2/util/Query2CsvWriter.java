package Query2.util;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.execution.datasources.csv.CSVUtils;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

public class Query2CsvWriter {

    public static final char CSV_SEPARATOR = ',';

    public static void makeCsv(JavaPairRDD<String, ArrayList<Tuple2<String, Double>>> rdd1, JavaPairRDD<String, ArrayList<Tuple2<String, Double>>> rdd2,
                               JavaPairRDD<String, ArrayList<Tuple3<String, Integer, Integer>>> rdd3, String output) throws IOException{
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
        writeCsv(list, output);
    }

    public static void writeCsv(List<Tuple2<String, ArrayList<Tuple2<String, Tuple4<Double, Double, Integer, Integer>>>>> l, String output) throws IOException{

        BufferedWriter writer = new BufferedWriter(new FileWriter(output));
        l.forEach(t -> {
            try {
                for (int i = 0; i < t._2().size(); i++) {
                    writer.append(t._1()).append(CSV_SEPARATOR)
                            .append(t._2().get(i)._1()).append(CSV_SEPARATOR)
                            .append(Double.toString(t._2().get(i)._2()._1())).append(CSV_SEPARATOR)
                            .append(Double.toString(t._2().get(i)._2()._2())).append(CSV_SEPARATOR)
                            .append(Integer.toString(t._2().get(i)._2()._3())).append(CSV_SEPARATOR)
                            .append(Integer.toString(t._2().get(i)._2()._4())).append(System.lineSeparator());

                }

            } catch (IOException io) {
                io.printStackTrace();
                System.out.println("Errore del file\n");
            }
        });
        writer.flush();
        writer.close();
    }

}
