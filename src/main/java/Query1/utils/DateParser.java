package Query1.utils;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public class DateParser {

    private final static String pattern = "yyyy-MM-dd'T'HH:mm:ss";

    public static DateTime dateTimeParser(String date) {
        DateTimeFormatter dtf = DateTimeFormat.forPattern(pattern);
        DateTime dateTime = dtf.parseDateTime(date);
        return dateTime;
    }
}
