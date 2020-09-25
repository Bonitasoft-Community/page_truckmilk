package org.bonitasoft.truckmilk.engine;

import java.io.InputStream;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInMeasurement;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeMeasure;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.toolbox.TypesCast;

public class MilkJobOutput {

    private final static String LOGGER_LABEL = "MilkJobOutput";
    private final static Logger logger = Logger.getLogger(MilkJobOutput.class.getName());

    public Date executionDate = new Date();
    /**
     * main result of the execution is a list of Events and the status.
     * Null until a new execution is started
     */
    private List<BEvent> listEvents = new ArrayList<>();

    // the listEvents is saved in the St
    public String listEventsSt = "";
    /**
     * give a simple status execution.
     */
    public ExecutionStatus executionStatus = ExecutionStatus.NOEXECUTION;

    /** save the time need to the execution */
    public Long executionTimeInMs;

    public MilkJob milkJob;

    /**
     * synthetic report on execution
     */
    StringBuilder report = new StringBuilder();

    /**
     * host name where execution was done
     */
    public String hostName;
    /*
     * return as information, how many item the plug in managed
     */
    public long nbItemsProcessed = 0;

    /**
     * this can be call by the CheckJobEnvironment method
     * Default Constructor.
     */
    public MilkJobOutput() {
        this.executionStatus = ExecutionStatus.SUCCESS;
    }

    // if you have a PlugInTourInput, create the object from this
    public MilkJobOutput(MilkJob milkJob) {
        this.milkJob = milkJob;
    }

    public void addEvent(BEvent event) {
        listEvents.add(event);
    }

    public void addEvents(List<BEvent> events) {
        listEvents.addAll(events);
    }

    public List<BEvent> getListEvents() {
        return listEvents;
    }

    public void setNbItemsProcessed(int nbItemsProcessed) {
        this.nbItemsProcessed = nbItemsProcessed;
    }

    /**
     * Set a parameterStream ==> WRITE
     * @param param
     * @param stream
     */
    public void setParameterStream(PlugInParameter param, InputStream stream) {
        if (param.typeParameter == TypeParameter.FILEWRITE || param.typeParameter == TypeParameter.FILEREADWRITE) {
            // update the PLUGIN parameters
            milkJob.setParameterStream(param, stream);
        } else {
            logger.severe(LOGGER_LABEL + "setParameterStream not allowed on parameter[" + param.name + "] (plugin " + milkJob.getName() + "]");
        }
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* collect Report of the execution, to give a synthesis to administrator */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    /**
     * Note : the header my be null, then the table does not have a first th line
     * 
     * @param header
     */
    public void addReportTableBegin(String[] header) {

        this.numberOfColumnsinTable = header == null ? 0 : header.length;

        StringBuilder tableHeader = new StringBuilder();

        tableHeader.append("<table class=\"table table-striped table-bordered table-sm\">");
        if (header != null) {
            tableHeader.append("<tr>");
            for (String col : header)
                tableHeader.append("<th>" + col + "</th>");
            tableHeader.append("</tr>");
        }
        addReportInHtml(tableHeader.toString());
        this.reportInATable = true;

    }

    private int limitNumberOfLines;
    private int countLinesInTable;
    private int numberOfColumnsinTable;

    /**
     * Start a table with a limiter. When the number of line access the limitter, then the report stop to store lines, and at the end, a message is set to
     * display the number of line produced
     * 
     * @param header
     * @param limitNumberOfLines
     */
    public void addReportTableBegin(String[] header, int limitNumberOfLines) {
        this.limitNumberOfLines = limitNumberOfLines;
        this.countLinesInTable = 0;
        addReportTableBegin(header);

    }

    /**
     * Add a new line in the report table
     * 
     * @param values
     */
    public void addReportTableLine(Object[] values) {
        countLinesInTable++;

        if (limitNumberOfLines > 0 && countLinesInTable >= limitNumberOfLines) {
            return; // no nothing
        }
        StringBuilder tableLines = new StringBuilder();

        tableLines.append("<tr>");
        NumberFormat nf = NumberFormat.getInstance();
        
                
        for (Object colValue : values) {
            if (colValue instanceof Long || colValue instanceof Integer || colValue instanceof Double)
                tableLines.append("<td style=\"text-align:right\">" + nf.format(colValue) + "</td>");
            else
                tableLines.append("<td>" + colValue + "</td>");
        }
        tableLines.append("</tr>");
        addReportInHtml(tableLines.toString());

    }

    public void addReportTableEnd() {
        if (limitNumberOfLines > 0 && countLinesInTable >= limitNumberOfLines) {
            addReportInHtml("<tr><td colspan=\"" + numberOfColumnsinTable + "\">Too many lines...(" + countLinesInTable + " lines)</td></tr>");
        }
        limitNumberOfLines = 0; // stop the limiter
        addReportInHtml("</table>");
    }

    public final static int CST_MAXSIZEINREPORT = 20000; // 20 ko; // 50 Ko
    boolean alreadyReportOverload = false;
    boolean reportInATable = false;

    /**
     * Add the line in the current report
     * 
     * @param htmlLine
     * @return true if the line is added, false if the report is overloaded
     */
    public boolean addReportInHtml(String htmlLine) {
        if (this.report.length() > CST_MAXSIZEINREPORT) {
            if (!this.alreadyReportOverload) {
                if (this.reportInATable)
                    this.report.append("</table>");
                this.report.append("Too many lines...");
            }

            this.alreadyReportOverload = true;
            return false;
        }
        this.report.append(htmlLine);
        return true;
    }

    public boolean addReportInHtmlCR(String htmlLine) {
        if (this.report.length() > CST_MAXSIZEINREPORT) {
            if (!this.alreadyReportOverload) {
                if (this.reportInATable)
                    this.report.append("</table>");
                this.report.append("Too many lines...");
            }

            this.alreadyReportOverload = true;
            return false;
        }
        this.report.append(htmlLine).append("<br>");
        return true;
    }

    public String getReportInHtml() {
        return report.toString();
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Want to register execution time in your plug in ? */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    public static class Chronometer {

        private long beginTime;
        private long endTime;
        private String operationName;

        public Chronometer(String operationName) {
            this.operationName = operationName;
            this.beginTime = System.currentTimeMillis();
        }

        /**
         * Must call MilkJobOutput.endMarker()
         */
        protected void stopTracker() {
            endTime = System.currentTimeMillis();
        }

        public long getTimeExecution() {
            return endTime - beginTime;
        }
    }

    private Map<String, Long> sumTimeOperation = new HashMap<>();
    private Map<String, Long> nbOccurencesOperation = new HashMap<>();

    public Chronometer beginChronometer(String operationName) {
        return new Chronometer(operationName);
    }

    /**
     * return the time in MS of this mark (diff between the startMarker and the endMarker) and collect it
     * 
     * @param marker
     * @param the chronometer may have registered multiple operation.
     * @return
     */
    public long endChronometer(Chronometer marker, int nbOccurenceOperation) {
        if (marker == null)
            return 0;
        long sumTime = sumTimeOperation.get(marker.operationName) == null ? 0 : sumTimeOperation.get(marker.operationName);
        long nbOccurences = nbOccurencesOperation.get(marker.operationName) == null ? 0 : nbOccurencesOperation.get(marker.operationName);
        marker.stopTracker();
        sumTimeOperation.put(marker.operationName, sumTime + marker.getTimeExecution());
        nbOccurencesOperation.put(marker.operationName, nbOccurences + nbOccurenceOperation);
        return marker.getTimeExecution();
    }

    /**
     * return the time in MS of this mark (diff between the startMarker and the endMarker) and collect it
     * 
     * @param marker
     * @return
     */
    public long endChronometer(Chronometer marker) {
        return endChronometer(marker, 1);
    }

    /**
     * add some time in a dedicated chronometer
     * 
     * @param operationName
     * @param timeInMs
     */
    public void addTimeChronometer(String operationName, long timeInMs) {
        long sumTime = sumTimeOperation.get(operationName) == null ? 0 : sumTimeOperation.get(operationName);
        long nbOccurences = nbOccurencesOperation.get(operationName) == null ? 0 : nbOccurencesOperation.get(operationName);

        sumTimeOperation.put(operationName, sumTime + timeInMs);
        nbOccurencesOperation.put(operationName, nbOccurences + 1);
    }

    public String collectPerformanceSynthesis(boolean keepOperationNoOccurrence) {
        StringBuilder result = new StringBuilder();
        for (String operationName : sumTimeOperation.keySet()) {
            long sumTime = sumTimeOperation.get(operationName);
            long nbOccurences = nbOccurencesOperation.get(operationName);
            if (nbOccurences == 0 && !keepOperationNoOccurrence)
                continue;
            result.append(operationName + ": " + nbOccurences + " in " + TypesCast.getHumanDuration(sumTime, true) + " ms ");
            if (nbOccurences > 0)
                result.append("(" + sumTime / nbOccurences + " ms/operation)");
            result.append("; ");
        }
        return result.toString();
    }

    /**
     * Add the markers in the report
     * 
     * @param addNumberOfOccurrence if true, the number of occurrence is added in the report
     * @param addAverage if true, the average is added in the report (total time / numberOfOccurent
     */
    public void addChronometersInReport(boolean addNumberOfOccurrence, boolean addAverage) {
        addReportTableBegin(new String[] { "Chronometer", "Time" });
        for (String operationName : sumTimeOperation.keySet()) {
            long sumTime = sumTimeOperation.get(operationName);
            long nbOccurences = nbOccurencesOperation.get(operationName);
            if (nbOccurences == 0 && !addNumberOfOccurrence)
                continue;
            addReportTableLine(new Object[] { operationName, TypesCast.getHumanDuration(sumTime, true) });
            if (nbOccurences > 0 && addAverage)
                addReportTableLine(new Object[] { operationName + " ms/operation (" + nbOccurences + ")", sumTime / nbOccurences });
        }
        addReportTableEnd();
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Register a Statistics Information */
    /*
     * THis number is collected, and use to build a graph.
     * PointOfInterest should be describe in the PlugInDescrition
     */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    /**
     * Mesure are saved as a Double format, but the caller can ask to restitute them as LONG
     */
    private Map<PlugInMeasurement, Double> mesures = new HashMap<>();

    public void setMeasure(PlugInMeasurement poi, double value) {
        if (milkJob.getPlugIn().getDescription().containsMesure(poi))
            mesures.put(poi, value);

    }

    public Double getMesure(PlugInMeasurement poi) {
        return mesures.get(poi);
    }

    public Map<PlugInMeasurement, Double> getAllMesures() {
        return mesures;
    }

    public static SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:MM:ss");

    /**
     * Add measures in the report. Measures are saved in the Measure table, but can be add in the report to have "all in one place"
     * @param keepMesureValueNotDefine if true, a measure not defined in the execution is added in the report, with the value 0
     * @param withEmbededMeasure Some measure are embedded : number of items processed and time to execute. They can be added in the report.
     */
    public void addMeasuresInReport(boolean keepMesureValueNotDefine, boolean withEmbededMeasure) {
        addReportTableBegin(new String[] { "Measure", "Value" });

        for (PlugInMeasurement measure : milkJob.getPlugIn().getDescription().getMeasures()) {
            if (measure.isEmbeded && !withEmbededMeasure)
                continue;

            Double measureValue = getMesure(measure);
            if (measureValue == null && !keepMesureValueNotDefine)
                continue;

            if (measure.typeMeasure == TypeMeasure.DOUBLE)
                addReportTableLine(new Object[] { measure.name, measureValue == null ? Double.valueOf(0) : measureValue });
            else if (measure.typeMeasure == TypeMeasure.LONG)
                addReportTableLine(new Object[] { measure.name, measureValue == null ? Long.valueOf(0) : measureValue.longValue() });
            else if (measure.typeMeasure == TypeMeasure.DATE)
                addReportTableLine(new Object[] { measure.name, measureValue == null ? "" : sdf.format(new Date(measureValue.longValue())) });
            else
                addReportTableLine(new Object[] { measure.name, measureValue == null ? Double.valueOf(0) : measureValue });
        }
        addReportTableEnd();
    }

}
