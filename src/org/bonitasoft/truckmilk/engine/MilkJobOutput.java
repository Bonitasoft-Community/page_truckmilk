package org.bonitasoft.truckmilk.engine;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.ExecutionStatus;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.toolbox.TypesCast;

public class MilkJobOutput {

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

    public String explanation;

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
            MilkPlugIn.logger.severe("setParameterStream not allowed on parameter[" + param.name + "] (plugin " + milkJob.getName() + "]");
        }
    }
    

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Want to register execution time in your plug in ? */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    public static class StartMarker {

        private long beginTime;
        private String operationName;

        public StartMarker(String operationName) {
            this.operationName = operationName;
            this.beginTime = System.currentTimeMillis();
        }
    }

    private Map<String, Long> sumTimeOperation = new HashMap();
    private Map<String, Long> nbOccurencesOperation = new HashMap();

    public StartMarker getStartMarker(String operationName) {
        return new StartMarker(operationName);
    }

    public void endMarker(StartMarker marker) {
        long sumTime = sumTimeOperation.get(marker.operationName) == null ? 0 : sumTimeOperation.get(marker.operationName);
        long nbOccurences = nbOccurencesOperation.get(marker.operationName) == null ? 0 : nbOccurencesOperation.get(marker.operationName);
        sumTimeOperation.put(marker.operationName, sumTime + (System.currentTimeMillis() - marker.beginTime));
        nbOccurencesOperation.put(marker.operationName, nbOccurences + 1);
    }

    public String collectResult(boolean keepOperationNoOccurrence) {
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

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Register a Statistics Information                                                */
    /* THis number is collected, and use to build a graph.
     * PointOfInterest sould be describe in the PlugInDescrition
     */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    private Map<String,Double> pointsOfInterest = new HashMap<>();
    public void addPointOfInterest(String name, double value) {
        pointsOfInterest.put(name, value);
    }
    
    public Map<String,Double> getPointsOfInterest() {
        return pointsOfInterest;
    }
    
}