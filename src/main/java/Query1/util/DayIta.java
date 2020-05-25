package Query1.util;

import org.joda.time.DateTime;
import java.io.Serializable;

public class DayIta implements Serializable {

    private DateTime date;
    private String state;
    private int hospitalizedWithSymptoms;
    private int intensiveCare;
    private int totalHospedalized;
    private int homeIsolation;
    private int totalConfirmed; //totale positivi
    private int totalConfirmedVariance;
    private int newConfirmed;
    private int healedDischarged; //dimessi guariti
    private int deceased;
    private int totalCases; //totale casi
    private int swabs; //tamponi
    private int testedCases;
    private String itaNotes;
    private String engNotes;

    public DayIta(DateTime date, String state, int hospitalizedWithSymptoms, int intensiveCare,
                  int totalHospedalized, int homeIsolation, int totalConfirmed, int totalConfirmedVariance,
                  int newConfirmed, int healedDischarged, int deceased, int totalCases, int swabs,
                  int testedCases, String itaNotes, String engNotes) {

        this.date = date;
        this.state = state;
        this.hospitalizedWithSymptoms = hospitalizedWithSymptoms;
        this.intensiveCare = intensiveCare;
        this.totalHospedalized = totalHospedalized;
        this.homeIsolation = homeIsolation;
        this.totalConfirmed = totalConfirmed;
        this.totalConfirmedVariance = totalConfirmedVariance;
        this.newConfirmed = newConfirmed;
        this.healedDischarged = healedDischarged;
        this.deceased = deceased;
        this.totalCases = totalCases;
        this.swabs = swabs;
        this.testedCases = testedCases;
        this.itaNotes = itaNotes;
        this.engNotes = engNotes;
    }

    public DayIta(DateTime date, String state, int hospitalizedWithSymptoms, int intensiveCare,
                  int totalHospedalized, int homeIsolation, int totalConfirmed, int totalConfirmedVariance,
                  int newConfirmed, int healedDischarged, int deceased, int totalCases, int swabs) {

        this.date = date;
        this.state = state;
        this.hospitalizedWithSymptoms = hospitalizedWithSymptoms;
        this.intensiveCare = intensiveCare;
        this.totalHospedalized = totalHospedalized;
        this.homeIsolation = homeIsolation;
        this.totalConfirmed = totalConfirmed;
        this.totalConfirmedVariance = totalConfirmedVariance;
        this.newConfirmed = newConfirmed;
        this.healedDischarged = healedDischarged;
        this.deceased = deceased;
        this.totalCases = totalCases;
        this.swabs = swabs;

    }

    public DateTime getDate() {
        return date;
    }

    public String getState() {
        return state;
    }

    public int getHospitalizedWithSymptoms() {
        return hospitalizedWithSymptoms;
    }

    public int getIntensiveCare() {
        return intensiveCare;
    }

    public int getTotalHospedalized() {
        return totalHospedalized;
    }

    public int getHomeIsolation() {
        return homeIsolation;
    }

    public int getTotalConfirmed() {
        return totalConfirmed;
    }

    public int getTotalConfirmedVariance() {
        return totalConfirmedVariance;
    }

    public int getNewConfirmed() {
        return newConfirmed;
    }

    public int getHealedDischarged() {
        return healedDischarged;
    }

    public int getDeceased() {
        return deceased;
    }

    public int getTotalCases() {
        return totalCases;
    }

    public int getSwabs() {
        return swabs;
    }

    public int getTestedCases() {
        return testedCases;
    }

    public String getItaNotes() {
        return itaNotes;
    }

    public String getEngNotes() {
        return engNotes;
    }


}

