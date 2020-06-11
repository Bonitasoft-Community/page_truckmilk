package org.bonitasoft.truckmilk.engine;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.plugin.MilkPing;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeMeasure;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInMeasurement;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.job.MilkJob;
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

    // if you have a PlugInTourInput, create the object from this
    public MilkJobOutput(MilkJob plugInTour) {
        this.milkJob = plugInTour;
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

    public void setParameterStream(PlugInParameter param, InputStream stream) {
        if (param.typeParameter == TypeParameter.FILEWRITE || param.typeParameter == TypeParameter.FILEREADWRITE) {
            // update the PLUGIN parameters
            milkJob.setParameterStream(param, stream);
        } else {
            logger.severe("setParameterStream not allowed on parameter[" + param.name + "] (plugin " + milkJob.getName() + "]");
        }
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* collect Report of the execution, to give a synthesis to administrator */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    public void addReportInHtml(String reportLine) {
        this.report.append(reportLine + "\n");
    }

    /**
     * Note : the header my be null, then the table does not have a first th line
     * 
     * @param header
     */
    public void addReportTable(String[] header) {
        this.report.append("<table class=\"table table-striped table-bordered table-sm\">");
        if (header != null) {
            this.report.append("<tr>");
            for (String col : header)
                this.report.append("<th>" + col + "</th>");
            this.report.append("</tr>");
        }
    }

    public void addReportLine(Object[] values) {
        this.report.append("<tr>");
        for (Object colValue : values) {
            if (colValue instanceof Long || colValue instanceof Integer || colValue instanceof Double)
                this.report.append("<td style=\"text-align:right\">" + colValue + "</td>");
            else
                this.report.append("<td>" + colValue + "</td>");
        }
        this.report.append("</tr>");
    }

    public void addReportLine(String htmlLine) {
        this.report.append(htmlLine);
    }

    public void addReportEndTable() {
        this.report.append("</table>");
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
     * return the time in MS of this mark (diff between the startMarker and the endMarker)
     * 
     * @param marker
     * @return
     */
    public long endChronometer(Chronometer marker) {
        if (marker == null)
            return 0;
        long sumTime = sumTimeOperation.get(marker.operationName) == null ? 0 : sumTimeOperation.get(marker.operationName);
        long nbOccurences = nbOccurencesOperation.get(marker.operationName) == null ? 0 : nbOccurencesOperation.get(marker.operationName);
        marker.stopTracker();
        sumTimeOperation.put(marker.operationName, sumTime + marker.getTimeExecution());
        nbOccurencesOperation.put(marker.operationName, nbOccurences + 1);
        return marker.getTimeExecution();
    }

    /**
     * add some time in a dedicated chronometer
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
     * @param keepOperationNoOccurrence
     */
    public void addChronometersInReport(boolean keepOperationNoOccurrence, boolean addAverage) {
        addReportTable(new String[] { "Chronometer", "Time" });
        for (String operationName : sumTimeOperation.keySet()) {
            long sumTime = sumTimeOperation.get(operationName);
            long nbOccurences = nbOccurencesOperation.get(operationName);
            if (nbOccurences == 0 && !keepOperationNoOccurrence)
                continue;
            addReportLine(new Object[] { operationName, TypesCast.getHumanDuration(sumTime, true) });
            if (nbOccurences > 0 && addAverage)
                addReportLine(new Object[] { operationName + " ms/operation ("+nbOccurences+")", sumTime / nbOccurences });
        }
        addReportEndTable();
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

    public void addMeasuresInReport(boolean keepMesureValueNotDefine, boolean withEmbededMeasure) {
        addReportTable(new String[] { "Measure", "Value" });

        for (PlugInMeasurement measure : milkJob.getPlugIn().getDescription().getMeasures()) {
            if (measure.isEmbeded && ! withEmbededMeasure) 
                continue;
            
            Double measureValue = getMesure(measure);
            if (measureValue == null && !keepMesureValueNotDefine)
                continue;

            if (measure.typeMeasure == TypeMeasure.DOUBLE)
                addReportLine(new Object[] { measure.name, measureValue == null ? Double.valueOf(0) : measureValue });
            else if (measure.typeMeasure == TypeMeasure.LONG)
                addReportLine(new Object[] { measure.name, measureValue == null ? Long.valueOf(0) : measureValue.longValue() });
            else if (measure.typeMeasure == TypeMeasure.DATE)
                addReportLine(new Object[] { measure.name, measureValue == null ? "" : sdf.format(new Date(measureValue.longValue())) });
            else
                addReportLine(new Object[] { measure.name, measureValue == null ? Double.valueOf(0) : measureValue });
        }
        addReportEndTable();
    }

}
