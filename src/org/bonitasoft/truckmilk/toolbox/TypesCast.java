package org.bonitasoft.truckmilk.toolbox;

import java.text.SimpleDateFormat;

public class TypesCast {

    /**
     * to share this date format in all the page
     */
    public static SimpleDateFormat sdfCompleteDate = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

    public static String getString(Object value, String defaultValue) {
        try {
            return (String) value;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static Boolean getBoolean(Object value, Boolean defaultValue) {
        try {
            return Boolean.valueOf(value.toString());
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static Integer getInteger(Object value, Integer defaultValue) {
        try {
            return Integer.valueOf(value.toString());
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * default toolbox method
     * 
     * @param value
     * @param defaultValue
     * @return
     */
    public static Long getLong(Object value, Long defaultValue) {
        try {
            return Long.valueOf(value.toString());
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static String getHumanDuration(long durationInMsn, boolean withMs) {
        String result = "";
        long timeRes = durationInMsn;
        long nbDays = timeRes / (1000 * 60 * 60 * 24);
        if (nbDays > 1)
            result += nbDays + " days ";

        timeRes = timeRes - nbDays * (1000 * 60 * 60 * 24);

        long nbHours = timeRes / (1000 * 60 * 60);
        result += String.format("%02d", nbHours) + ":";
        timeRes = timeRes - nbHours * (1000 * 60 * 60);

        long nbMinutes = timeRes / (1000 * 60);
        result += String.format("%02d", nbMinutes) + ":";
        timeRes = timeRes - nbMinutes * (1000 * 60);

        long nbSecond = timeRes / (1000);
        result += String.format("%02d", nbSecond) + " ";
        timeRes = timeRes - nbSecond * (1000);

        if (withMs)
            result += String.format("%03d", timeRes);
        return result;
    }
}
