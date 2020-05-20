package Query1.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CsvOutput {

    public static void printOutputOnCsv(String outputFile, ArrayList<List> myAverageRdds) {

        try {
            File file = new File(outputFile);
            FileWriter writer = new FileWriter(file);
            writer.write("data,dimessi_guariti,tamponi\n");
            for (List l : myAverageRdds) {
                List<String> strings = new ArrayList<>();
                strings.add(l.get(0).toString());
                strings.add(l.get(1).toString());
                strings.add(l.get(2).toString());
                String collect = strings.stream().collect(Collectors.joining(","));
                writer.write(collect + "\n");
            }
            writer.close();
        } catch (IOException io) {
            io.printStackTrace();
            System.out.println("Errore di IO");
        }
    }
}
