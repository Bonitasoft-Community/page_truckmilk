package org.bonitasoft.truckmilk.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInMeasurement;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;

import org.json.simple.JSONValue;

import lombok.Data;

public @Data class MilkPlugInDescription {

    public final static PlugInMeasurement cstMesureTimeExecution         = PlugInMeasurement.createInstance( "timeexecution", "Time Execution", "Time to execute the plug in (in ms)");
    public final static PlugInMeasurement cstMesureNbItemProcessed       = PlugInMeasurement.createInstance( "nbItemsprocessed", "Nb items processed", "Number of item the job processed during the execution");

    String name;
    private String version;
    String label;

    
    public enum CATEGORY {
        TASKS, CASES, MONITOR, OTHER
    };

    MilkPlugInDescription.CATEGORY category = CATEGORY.OTHER;
    /**
     * explanation is given by the plug in, user can't change it
     */
    private String explanation;
    private List<PlugInParameter> inputParameters = new ArrayList<>();
    private List<PlugInParameter> analysisParameters = new ArrayList<>();

    private String cronSt = "0 0/10 * 1/1 * ? *"; // every 10 mn
    public enum JOBSTOPPER { NO, MAXMINUTES, MAXITEMS, BOTH }
    private JOBSTOPPER jobCanBeStopped = JOBSTOPPER.NO;
    Integer jobStopMaxItems = null;
    Integer jobStopMaxTimeMn = null;

    /**
     * Plugin can set a special warning, to warm use on special information
     */
    private String warning=null;
    
    public MilkPlugInDescription() {
        addMesure( cstMesureTimeExecution );
        addMesure( cstMesureNbItemProcessed );
    }
    
    
    public boolean isJobCanBeStopByMaxMinutes() {
        return jobCanBeStopped == JOBSTOPPER.MAXMINUTES || jobCanBeStopped == JOBSTOPPER.BOTH;
    }
    public boolean isJobCanBeStopByMaxItems() {
        return jobCanBeStopped == JOBSTOPPER.MAXITEMS || jobCanBeStopped == JOBSTOPPER.BOTH;
    }

    public void addParameterSeparator(String title,String explanation) {
        addParameter( PlugInParameter.createInstance(title, title, TypeParameter.SEPARATOR, "", explanation) );
    }
    public void addParameter(PlugInParameter parameter) {
        inputParameters.add(parameter);
    }

    public void addAnalysisParameter(PlugInParameter parameter) {
        analysisParameters.add(parameter);
    }

    
     
    /**
     * Expected format :
     * {"delayinmn":10, "delayinmn_label="Delai in minutes", "maxtentative":12,"processfilter":[{"name":"expens*","version":null}]})
     * Note: the label has the same name as the key + "_label", or is the key
     * 
     * @param jsonSt
     */
    @SuppressWarnings("unchecked")
    public void addParameterFromMapJson(String jsonSt) {
        Map<String, Object> mapJson = (Map<String, Object>) JSONValue.parse(jsonSt);
        for (String key : mapJson.keySet()) {
            if (key.endsWith("_label"))
                continue;
            Object label = mapJson.get(key + "_label");
            if (label == null)
                label = key;
            addParameter(PlugInParameter.createInstance(key, label.toString(), TypeParameter.STRING, mapJson.get(key), null));
        }
    }

    public Map<String, Object> getParametersMap() {
        Map<String, Object> map = new HashMap<>();
        for (PlugInParameter plugInParameter : inputParameters) {
            map.put(plugInParameter.name, plugInParameter.defaultValue);
        }
        return map;
    }

    /**
     * return a parameter from it's name, null if not exist
     * 
     * @param paramName
     * @return
     */
    public PlugInParameter getPlugInParameter(String paramName) {
        for (PlugInParameter plugInParameter : inputParameters) {
            if (plugInParameter.name.equals(paramName))
                return plugInParameter;
        }
        return null;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Mesures */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */


    /**
     * Description has to describe the different mesure defined
     * order is important to display it.
     */
    private List<PlugInMeasurement> listMesures = new ArrayList<>();
    private Map<String,PlugInMeasurement> mapMesures = new HashMap<>();
    
    public void addMesure( PlugInMeasurement mesure ) {
        if (! mapMesures.containsKey(mesure.getName())) {
            mapMesures.put( mesure.getName(), mesure );
            listMesures.add(mesure);
        }
    }
    public List<PlugInMeasurement> getMeasures() {
        return listMesures;
    }

    public boolean containsMesure(PlugInMeasurement poi ) {
        return mapMesures.containsKey( poi.getName());
    }
    public boolean hasMesures() {
        return ! mapMesures.isEmpty();
    }
    public PlugInMeasurement getMesureFromName(String name ) {
        return mapMesures.get( name );
    }
       
    
    

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Job Stopper */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    public void setStopJob( JOBSTOPPER jobCanBeStopped)
    {
        this.jobCanBeStopped = jobCanBeStopped;
    }
    
    public void setStopAfterMaxItems( int maxItems) 
    {
        this.jobStopMaxItems = maxItems;
    }
    public Integer getStopAfterMaxItems() {
        return this.jobStopMaxItems;
    }
    public void setStopAfterMaxTime( int maxTimeMn) 
    {
        this.jobStopMaxTimeMn = maxTimeMn;
    }
    public Integer getStopAfterMaxTime() {
        return this.jobStopMaxTimeMn;
    }

}