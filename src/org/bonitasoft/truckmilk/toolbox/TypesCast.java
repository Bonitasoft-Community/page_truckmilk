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

}
