package Query1.utils;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.AbstractRowProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;

// Let's create our own RowProcessor to analyze the rows
public class CsvDimension extends AbstractRowProcessor {

    int lastColumn = -1;
    int rowCount = 0;

    @Override
    public void rowProcessed(String[] row, ParsingContext context) {
        rowCount++;
        if (lastColumn < row.length) {
            lastColumn = row.length;
        }
    }


    public static ArrayList<Integer> computeCsvDimensions (String pathToFile) throws FileNotFoundException {
        // let's measure the time roughly
        long start = System.currentTimeMillis();

        File file = new File(pathToFile);

        //Creates an instance of our own custom RowProcessor, defined above.
        CsvDimension myDimensionProcessor = new CsvDimension();

        CsvParserSettings settings = new CsvParserSettings();

        //This tells the parser that no row should have more than 2,000,000 columns
        settings.setMaxColumns(20);

        //Here you can select the column indexes you are interested in reading.
        //The parser will return values for the columns you selected, in the order you defined
        //By selecting no indexes here, no String objects will be created
        settings.selectIndexes();

        //When you select indexes, the columns are reordered so they come in the order you defined.
        //By disabling column reordering, you will get the original row, with nulls in the columns you didn't select
        settings.setColumnReorderingEnabled(false);

        //We instruct the parser to send all rows parsed to your custom RowProcessor.
        settings.setRowProcessor(myDimensionProcessor);

        //Finally, we create a parser
        CsvParser parser = new CsvParser(settings);

        //And parse! All rows are sent to your custom RowProcessor (CsvDimension)
        //I'm using a 150MB CSV file with 1.3 million rows.
        parser.parse(new FileReader(file));

        ArrayList<Integer> result = new ArrayList<Integer>();
        result.add(myDimensionProcessor.lastColumn);
        result.add(myDimensionProcessor.rowCount);

        //Nothing else to do. The parser closes the input and does everything for you safely. Let's just get the results:
        //System.out.println("Columns: " + result.get(0));
        //System.out.println("Rows: " + result.get(1));
        //System.out.println("Time taken: " + (System.currentTimeMillis() - start) + " ms");

        return result;
    }

    public static int countNumRows(String pathToFile) throws FileNotFoundException{

        ArrayList<Integer> dimensions = computeCsvDimensions(pathToFile);
        int rows = dimensions.get(1)-1;

        return rows;
    }
}