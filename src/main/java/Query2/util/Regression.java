package Query2.util;

import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.util.ArrayList;

public class Regression {

    public static Double getSlope(ArrayList<Integer> arrayList) {
        SimpleRegression simpleRegression = new SimpleRegression();
        for(int i = 0; i < arrayList.size(); i++) {
            simpleRegression.addData(Double.valueOf(i), Double.valueOf(arrayList.get(i)));
        }
        return simpleRegression.getSlope();

    }
}
