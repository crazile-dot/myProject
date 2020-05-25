package Query1.util;

import org.apache.spark.api.java.JavaPairRDD;
import org.joda.time.DateTime;
import scala.Tuple2;
import scala.Tuple4;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Query1CsvWriter {

    public static final char CSV_SEPARATOR = ',';

    public static void makeCsv(JavaPairRDD<DateTime, Double> rdd1, JavaPairRDD<DateTime, Double> rdd2, String output) throws IOException{
        //aggiungere un controllo (se esiste già il file, ciao)
        JavaPairRDD<DateTime, Tuple2<Double, Double>> joinedRdd = rdd1.join(rdd2).sortByKey();
        List<Tuple2<DateTime, Tuple2<Double, Double>>> list = joinedRdd.collect();
        writeCsv(list, output);
    }

    public static void writeCsv(List<Tuple2<DateTime, Tuple2<Double, Double>>> l, String output) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(output));
        l.forEach(t -> {
            try {
                writer.append(t._1().toString()).append(CSV_SEPARATOR)
                        .append(Double.toString(t._2()._1())).append(CSV_SEPARATOR)
                        .append(Double.toString(t._2()._2())).append(System.lineSeparator());
            } catch (IOException io) {
                io.printStackTrace();
                System.out.println("Errore del file\n");
            }
        });
        writer.flush();
        writer.close();
    }
}
