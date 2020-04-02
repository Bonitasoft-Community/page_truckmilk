package org.bonitasoft.truckmilk.toolbox;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Logger;

public class MilkLog {

    private Logger logger;
    public String loggerClassName;
    public String shortClassName;

    public static MilkLog getLogger(String loggerClassName) {
        return new MilkLog(loggerClassName);
    }

    private MilkLog(String loggerClassName) {
        this.loggerClassName = loggerClassName;
        shortClassName = loggerClassName;
        if (shortClassName.lastIndexOf(".") != -1)
            shortClassName = shortClassName.substring(shortClassName.lastIndexOf(".") + 1);
        logger = Logger.getLogger(loggerClassName);
    }

    public void info(String msg) {
        logger.info(getLogHeader() + msg);
    }

    public void severe(String msg) {
        logger.severe(getLogHeader() + msg);
    }

    public void severeException(Exception e, String msg) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        String exceptionDetails = sw.toString();
        logger.severe(getLogHeader() + msg + "Exception " + e.getMessage() + " at " + exceptionDetails);
    }

    public void fine(String msg) {
        logger.fine(getLogHeader() + msg);
    }

    private String getLogHeader() {
        return "#" + Thread.currentThread().getName() + " TruckMilk." + shortClassName + ": ";
    }
}
