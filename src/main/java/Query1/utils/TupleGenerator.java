package Query1.utils;

import org.joda.time.DateTime;
import scala.Tuple13;
import scala.Tuple3;

public class TupleGenerator {

    public static Tuple13 getTuple13(Day_Ita day_ita) {

        Tuple13<DateTime, String, Integer, Integer, Integer, Integer, Integer,
                Integer, Integer, Integer, Integer, Integer, Integer> tuple13 =
                new Tuple13<>(day_ita.getDate(), day_ita.getState(), day_ita.getHospitalizedWithSymptoms(),
                        day_ita.getIntensiveCare(), day_ita.getTotalHospedalized(), day_ita.getHomeIsolation(),
                        day_ita.getTotalConfirmed(), day_ita.getTotalConfirmedVariance(), day_ita.getNewConfirmed(),
                        day_ita.getHealedDischarged(), day_ita.getDeceased(), day_ita.getTotalCases(), day_ita.getSwabs());

        return tuple13;
    }

    public static Tuple3 getTuple3(Day_Ita day_ita) {

        Tuple3<DateTime, Integer, Integer> tuple3 = new Tuple3<>(day_ita.getDate(),
                day_ita.getHealedDischarged(), day_ita.getSwabs());

        return tuple3;
    }
}
