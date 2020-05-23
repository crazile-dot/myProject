package util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class DateParser {

    private final static String pattern = "yyyy-MM-dd'T'HH:mm:ss";

    public static DateTime dateTimeParser(String date) {
        DateTimeFormatter dtf = DateTimeFormat.forPattern(pattern);
        DateTime dateTime = dtf.parseDateTime(date);
        return dateTime;
    }

    public static Date dateParser(String date) throws ParseException {
        Date myDate =new SimpleDateFormat("MM/dd/yyyy").parse(date);
        return myDate;
    }
}
