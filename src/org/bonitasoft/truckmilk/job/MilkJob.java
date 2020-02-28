package org.bonitasoft.truckmilk.job;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.engine.MilkJobFactory;
import org.bonitasoft.truckmilk.engine.MilkJobFactory.MilkFactoryOp;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.ExecutionStatus;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugTourOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugInFactory;
import org.bonitasoft.truckmilk.toolbox.MilkLog;
import org.bonitasoft.truckmilk.toolbox.TypesCast;
import org.json.simple.JSONValue;
import org.quartz.CronExpression;

/* ******************************************************************************** */
/*                                                                                  */
/* PlugInTour */
/*                                                                                  */
/*
 * A new plugin instance is requested, to be start every 5 mn for example.
 * A plugInTour reference a PlugIn (embeded or not), a Schedule (frequency and next start) and
 * parameters
 */
/*                                                                                  */
/* ******************************************************************************** */

public class MilkJob {

    public static MilkLog logger = MilkLog.getLogger(MilkJob.class.getName());

    private static BEvent eventCronParseError = new BEvent(MilkJob.class.getName(), 1, Level.APPLICATIONERROR,
            "Bad cron expression ", "Cron expression is not correct", "The next date can't be calculated",
            "Give a correct date");

    private static BEvent EVENT_ERROR_READING_PARAMFILE = new BEvent(MilkJob.class.getName(), 3, Level.APPLICATIONERROR,
            "Error reading Parameter File", "An error occure during the reading of a parameters file", "Content can't be accessible",
            "Check the log");

    private static BEvent EVENT_CANT_FIND_TEMPORARY_PATH = new BEvent(MilkJob.class.getName(), 4, Level.ERROR,
            "Can't find temporary path", "File are uploaded in a temporary path. This path can't be found", "Content will not be updated",
            "Check the temporary path");

    private static BEvent EVENT_CANT_FIND_TEMPORARY_FILE = new BEvent(MilkJob.class.getName(), 5, Level.ERROR,
            "Can't find temporary file", "File are uploaded in a temporary path. File is not found in the temporary path", "Content will not be updated",
            "Check the temporary path; the log. Maybe file is full?");

    public static String DEFAULT_NAME = "";
    /**
     * the reference to the object to execute the Tour
     */
    public MilkPlugIn plugIn;

    /**
     * register the tour factory which create this object
     */
    public MilkJobFactory milkJobFactory;

    private static String cstPrefixStreamValue = "_st";
    /**
     * be compatible : old TruckMilk does not have Id
     */
    public long idJob = -1;

    public String name;
    public String description;

    public boolean isEnable = false;

    public String cronSt = "";

    /**
     * list of host name or ip address separate by a ;
     * public String hostRestriction;
     * /**
     * in a Cluster environment, we may want this plugInTour is executed only on a specific node.
     */
    public String hostsRestriction = null;

    /**
     * save the Value for all parameters, even STREAM parameters (end with "_st")
     */
    public Map<String, Object> parameters = new HashMap<String, Object>();

    public MilkJob(String name, MilkPlugIn plugIn, MilkJobFactory milkJobFactory) {
        this.plugIn = plugIn;
        this.name = name == null ? MilkJob.DEFAULT_NAME : name;
        this.milkJobFactory = milkJobFactory;
        this.generateId();
    }

    public static MilkJob getInstanceFromPlugin(String name, MilkPlugIn plugIn, MilkJobFactory milkPlugInTourFactory) {
        MilkJob milkJob = new MilkJob(name, plugIn, milkPlugInTourFactory);
        PlugInDescription description = plugIn.getDescription();
        // clone the parameters !
        // new HashMap<>(description.getParametersMap()) not need at this moment because the maps is created
        milkJob.parameters = description.getParametersMap();
        milkJob.cronSt = description.cronSt;

        return milkJob;
    }

    public String toString() {
        return name + " (enable:" + isEnable + " immediateExecution:" + trackExecution.isImmediateExecution + " InExecution:" + trackExecution.inExecution + (trackExecution.nextExecutionDate == null ? " noNextDate" : " " + sdf.format(trackExecution.nextExecutionDate)) + ")";
    }

    public long getId() {
        return idJob;
    }

    public String getName() {
        return name == null ? MilkJob.DEFAULT_NAME : name;
    };

    public void setName(String name) {
        if (name == null)
            name = MilkJob.DEFAULT_NAME;
        this.name = name;
    }

    public MilkPlugIn getPlugIn() {
        return plugIn;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /*
     * Operation on tour
     * /*
     */
    /* ******************************************************************************** */

    /**
     * return a boolean value, and set a default one
     * 
     * @param value
     * @param defaultValue
     * @return
     */
    private static Boolean getBooleanValue(Object value, Boolean defaultValue) {
        if (value == null)
            return defaultValue;
        if (value instanceof Boolean)
            return (Boolean) value;
        try {
            return Boolean.valueOf(value.toString());
        } catch (Exception e) {
            // Nothing to log, this is acceptable to return the default value
        }
        return defaultValue;
    }

    private static Long getLongValue(Object value, Long defaultValue) {
        if (value == null)
            return defaultValue;
        if (value instanceof Long)
            return (Long) value;
        try {
            return Long.valueOf(value.toString());
        } catch (Exception e) {
            // Nothing to log, this is acceptable to return the default value
        }
        return defaultValue;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /*
     * Operation on plugin
     * /*
     */
    /* ******************************************************************************** */
    public List<BEvent> checkByPlugIn() {
        List<BEvent> listEvents = new ArrayList<BEvent>();
        if (parameters == null)
            parameters = new HashMap<String, Object>();
        // verify that all plugin parameters are in
        for (PlugInParameter plugInParameter : plugIn.getDescription().inputParameters) {
            if (!parameters.containsKey(plugInParameter.name)) {
                // new parameters
                parameters.put(plugInParameter.name, plugInParameter.defaultValue);
            }
        }
        return listEvents;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /*
     * execute a job
     * /*
     */
    /* ******************************************************************************** */

    public List<BEvent> calculateNextExecution() {
        List<BEvent> listEvents = new ArrayList<BEvent>();

        try {
            CronExpression cronExp = new CronExpression(cronSt);
            trackExecution.nextExecutionDate = cronExp.getNextValidTimeAfter(new Date());
        } catch (Exception e) {
            trackExecution.nextExecutionDate = null;
            listEvents.add(new BEvent(eventCronParseError, e, "Expression[" + cronSt + "]"));
        }
        return listEvents;
    }

    public List<BEvent> setCron(String cronSt) {
        this.cronSt = cronSt;
        return calculateNextExecution();
    }

    public void setHostsRestriction(String hostsRestriction) {
        this.hostsRestriction = hostsRestriction;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Date getNextExecution() {
        return trackExecution.nextExecutionDate;
    }

    /**
     * set the status. If enaablme, then the next execution is calculated according the cronSt given
     * 
     * @param enable
     */
    public List<BEvent> setEnable(boolean enable) {
        List<BEvent> listEvents = new ArrayList<BEvent>();
        isEnable = enable;
        if (isEnable)
            listEvents.addAll(calculateNextExecution());
        else
            trackExecution.nextExecutionDate = null;
        return listEvents;
    }

    /**
     * Next check ? Start immediately
     */
    public void setImmediateExecution(boolean immediateExecution) {
        trackExecution.isImmediateExecution = immediateExecution;

    }

    public boolean isImmediateExecution() {
        return trackExecution.isImmediateExecution;
    }

    /**
     * get the value of parameters for this tour (definition is accessible via
     * plugIn.getDescription().inputParameters
     * 
     * @return
     */
    public Map<String, Object> getTourParameters() {
        return parameters;
    }

    /**
     * @param parameters
     */
    public void setJobParameters(Map<String, Object> parameters) {
        this.parameters = parameters == null ? new HashMap<String, Object>() : parameters;
    }

    /**
     * set a Stream parameters
     * 
     * @param parameterName
     * @param temporaryFileName
     * @param pageDirectory
     */
    public List<BEvent> setJobFileParameter(String parameterName, String temporaryFileName, File pageDirectory) {
        List<BEvent> listEvents = new ArrayList<BEvent>();
        List<String> listParentTmpFile = new ArrayList<String>();
        try {
            listParentTmpFile.add(pageDirectory.getCanonicalPath() + "/../../../tmp/");
            listParentTmpFile.add(pageDirectory.getCanonicalPath() + "/../../");
        } catch (Exception e) {
            logger.info(".setTourFileParameter: error get CanonicalPath of pageDirectory[" + e.toString() + "]");
            listEvents.add(EVENT_CANT_FIND_TEMPORARY_PATH);
            return listEvents;
        }
        StringBuilder detectedPaths = new StringBuilder();
        boolean findFile = false;
        for (String pathTemp : listParentTmpFile) {
            // logger.fine(logHeader+".setTourFileParameter: CompleteuploadFile  TEST [" + pathTemp + temporaryFileName + "]");
            detectedPaths.append( "[" + pathTemp + temporaryFileName + "],");
            if (new File(pathTemp + temporaryFileName).exists()) {
                try {
                    findFile = true;
                    FileInputStream fileinputStream = new FileInputStream(pathTemp + temporaryFileName);
                    // search the parameters
                    PlugInDescription plugInDescription = getPlugIn().getDescription();

                    for (PlugInParameter parameter : plugInDescription.inputParameters) {
                        if (parameter.name.equals(parameterName) &&
                                (parameter.typeParameter == TypeParameter.FILEREAD || parameter.typeParameter == TypeParameter.FILEREADWRITE)) {
                            setParameterStream(parameter, fileinputStream);
                        }
                    }
                } catch (Exception e) {
                    logger.severeException(e, ".setTourFileParameter: File[" + pathTemp + temporaryFileName + "] ");
                    listEvents.add(new BEvent(EVENT_CANT_FIND_TEMPORARY_FILE, e, "Path:" + pathTemp + "]File[" + temporaryFileName + "] Complete file[" + pathTemp + temporaryFileName + "]"));

                }
            }
        } // end look on pathDirectory
        if (!findFile)
            listEvents.add(new BEvent(EVENT_CANT_FIND_TEMPORARY_FILE, "Path:" + pageDirectory.toString() + "]File[" + temporaryFileName + "] Detected path " + detectedPaths));
        return listEvents;
    }

    /**
     * if a hostRestriction is on place, then verify it
     * 
     * @return
     */
    public boolean isInsideHostsRestriction() {
        if (hostsRestriction == null || hostsRestriction.trim().length() == 0)
            return true; // no limitation

        String compareHostRestriction = ";" + hostsRestriction + ";";
        try {
            InetAddress ip = InetAddress.getLocalHost();

            if ((compareHostRestriction.indexOf(";" + ip.getHostAddress() + ";") != -1) || 
                (compareHostRestriction.indexOf(";" + ip.getHostName() + ";") != -1))
                return true;

            return false;
        } catch (UnknownHostException e) {
            return false;
        }
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /*
     * Parameter File
     * /*
     */
    /* ******************************************************************************** */

    /**
     * save/load FILE parameters
     * setParameterStream set the stream, i.e. WRITE the content to the plugInTour. After that, a
     * save() is mandatory to save it in the database
     * 
     * @param param
     * @param stream
     */
    public void setParameterStream(PlugInParameter plugInParameter, InputStream stream) {
        parameters.put(plugInParameter.name + cstPrefixStreamValue, stream);
    }

    /**
     * get a ParameterStream
     * 
     * @param plugInParameter
     * @return
     */
    public InputStream getParameterStream(PlugInParameter plugInParameter) {
        return (InputStream) parameters.get(plugInParameter.name + cstPrefixStreamValue);
    }

    /**
     * populateOutputStream , i.e. READ the content from the plugInTOur.
     * 
     * @param plugInParameter
     * @param stream
     * @return
     */
    public List<BEvent> getParameterStream(PlugInParameter plugInParameter, OutputStream stream) {
        List<BEvent> listEvents = new ArrayList<BEvent>();
        try {
            InputStream instream = (InputStream) parameters.get(plugInParameter.name + cstPrefixStreamValue);
            // the document is not uploaded - not consider as an error
            if (instream == null)
                return listEvents;
            byte[] buffer = new byte[10000];
            while (true) {
                int bytesRead;
                bytesRead = instream.read(buffer);
                if (bytesRead == -1)
                    break;
                stream.write(buffer, 0, bytesRead);
            }
        } catch (IOException e) {
            listEvents.add(new BEvent(EVENT_ERROR_READING_PARAMFILE, e, "ParameterFile[" + plugInParameter.name + "]"));
            logger.severeException(e, ".getParameterStream:Error writing parameter ");
        }
        return listEvents;
    }
    public final static String cstJsonCron = "cron";
    public final static String cstJsonHostsRestriction = "hostsrestriction";

    
    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * describe the plug tour
     * 
     * @return
     */

    private final static String cstJsonPrefixHumanReadable = "st";
    private final static String cstJsonPluginName = "pluginname";
    private final static String cstJsonEnable = "enable";

    private final static String CST_JSON_PARAMETERS_DEF = "parametersdef";
    private final static String cstJsonAnalysisDef = "analysisdef";

    private final static String cstJsonParameters = "parameters";
    // private final static String cstJsonParameterValue = "value";

    private final static String cstJsonPlugInDisplayName = "plugindisplayname";
    private final static String cstJsonDescription = "description";
    private final static String cstJsonExplanation = "explanation";
    private final static String cstJsonNextExecution = "nextexecution";
    private final static String cstJsonLastExecution = "lastexecution";
    private final static String cstJsonName = "name";
    private final static String cstJsonId = "id";
    private final static String cstJsonImmediateExecution = "imediateExecution";
    private final static String cstJsonAskForStop = "askForStop";

    private final static String cstJsonlastExecutionStatus = "lastexecutionstatus";
    private final static String cstJsonInExecution = "inExecution";

    private final static String cstJsonRegisterInExecutionHost = "inExecutionHost";
    private final static String cstJsonInExecutionStartTime = "inExecutionStartTime";
    private final static String cstJsonInExecutionPercent = "inExecutionPercent";
    private final static String cstJsonInExecutionEndTimeEstimatedInMS = "inExecutionEndTimeEstimatedinMS";
    private final static String cstJsonInExecutionEndDateEstimated = "inExecutionEndDateEstimated";

    // saved last execution  
    private final static String cstJsonSavedExec = "savedExecution";
    private final static String cstJsonSaveExecDate = "execDate";
    private final static String cstJsonSaveExecStatus = "status";
    private final static String cstJsonSaveExecListEventsSt = "listevents";
    private final static String cstJsonSaveExecExplanation = "explanation";
    private final static String cstJsonSaveExecItemsProcessed = "itemprocessed";
    private final static String cstJsonSaveExecTimeinMs = "timeinms";
    private final static String cstJsonSaveExecHostName = "hostname";

    /**
     * getInstanceFromMap (the load)
     * 
     * @see getMap()
     * @param jsonSt
     * @param milkCmdControl
     * @return
     */

    @SuppressWarnings("unchecked")
    public static MilkJob getInstanceFromJson(String jsonSt, MilkJobFactory milkPlugInTourFactory) {
        Map<String, Object> jsonMap = (Map<String, Object>) JSONValue.parse(jsonSt);
        if (jsonMap == null)
            return null;

        String plugInName = (String) jsonMap.get(cstJsonPluginName);
        MilkPlugInFactory milkPlugInFactory = milkPlugInTourFactory.getMilkPlugInFactory();

        MilkPlugIn plugIn = milkPlugInFactory.getPluginFromName(plugInName);
        if (plugIn == null)
            return null;

        String name = (String) jsonMap.get(cstJsonName);
        MilkJob milkJob = new MilkJob(name, plugIn, milkPlugInTourFactory);
        milkJob.description = (String) jsonMap.get(cstJsonDescription);

        milkJob.idJob = getLongValue(jsonMap.get(cstJsonId), 0L);

        // clone the parameters !
        // new HashMap<>(description.getParametersMap()) not need at this moment because the maps is created
        milkJob.parameters = (Map<String, Object>) jsonMap.get(cstJsonParameters);

        @SuppressWarnings("unused")
        List<Map<String, Object>> listParametersDef = (List<Map<String, Object>>) jsonMap.get(CST_JSON_PARAMETERS_DEF);

        milkJob.cronSt = (String) jsonMap.get(cstJsonCron);
        milkJob.hostsRestriction = (String) jsonMap.get(cstJsonHostsRestriction);

        // search the name if all the list
        milkJob.isEnable = getBooleanValue(jsonMap.get(cstJsonEnable), false);

        milkJob.trackExecution.askForStop = getBooleanValue(jsonMap.get(cstJsonAskForStop), false);

        Long lastExecutionDateLong = (Long) jsonMap.get(cstJsonLastExecution);
        if (lastExecutionDateLong != null && lastExecutionDateLong != 0)
            milkJob.trackExecution.lastExecutionDate = new Date(lastExecutionDateLong);

        String lastExecutionStatus = (String) jsonMap.get(cstJsonlastExecutionStatus);
        if (lastExecutionStatus != null)
            milkJob.trackExecution.lastExecutionStatus = ExecutionStatus.valueOf(lastExecutionStatus.toUpperCase());

        milkJob.trackExecution.isImmediateExecution = getBooleanValue(jsonMap.get(cstJsonImmediateExecution), false);
        milkJob.trackExecution.inExecution = getBooleanValue(jsonMap.get(cstJsonInExecution), false);
        milkJob.trackExecution.startTime = getLongValue(jsonMap.get(cstJsonInExecutionStartTime), 0L);
        milkJob.trackExecution.percent = getLongValue(jsonMap.get(cstJsonInExecutionPercent), 0L);
        milkJob.trackExecution.endTimeEstimatedInMs = getLongValue(jsonMap.get(cstJsonInExecutionEndTimeEstimatedInMS), 0L);
        milkJob.trackExecution.endDateEstimated = null;
        if (milkJob.trackExecution.startTime > 0 && milkJob.trackExecution.endTimeEstimatedInMs > 0) {
            milkJob.trackExecution.endDateEstimated = new Date(milkJob.trackExecution.startTime + milkJob.trackExecution.endTimeEstimatedInMs);
        }
        milkJob.trackExecution.inExecutionHostName = (String) jsonMap.get(cstJsonRegisterInExecutionHost);

        if (milkJob.isEnable && milkJob.trackExecution.nextExecutionDate == null)
            milkJob.calculateNextExecution();

        // get the last saved execution
        // nota: MilkSerialize saved this information in a different variable, then it overide this value by calling the getreadSavedExecution() methiod
        List<Map<String, Object>> list = (List<Map<String, Object>>) jsonMap.get(cstJsonSavedExec);
        if (list != null) {
            for (Map<String, Object> execSaveMap : list) {
                milkJob.listSavedExecution.add(SavedExecution.getInstance(execSaveMap));
            }
        }
        return milkJob;
    }

    /**
     * how to calcul the map
     * To send to the browser: everything
     * to serialize : depends, because different item are saved in different variable
     * 
     * @author Firstname Lastname
     */
    public static class MapContentParameter {

        boolean withExplanation = false;
        public boolean trackExecution = true;
        public boolean askStop = true;
        public boolean savedExecution = true;

        public static MapContentParameter getInstanceWeb() {
            MapContentParameter mapContentParameter = new MapContentParameter();
            mapContentParameter.withExplanation = true;
            mapContentParameter.withExplanation = true;
            mapContentParameter.askStop = true;
            mapContentParameter.savedExecution = true;
            return mapContentParameter;
        }
    }

    /**
     * getMap : use to save it or send to the browser.
     * browser need to have the parameters definition AND the value, save need only the parameter
     * value.
     * 
     * @see getInstanceFromJson
     * @param withExplanation
     * @param allJobInformation
     * @return
     */
    public Map<String, Object> getMap(MapContentParameter mapContent) {

        Map<String, Object> map = new HashMap<String, Object>();
        map.put(cstJsonName, getName());
        map.put(cstJsonId, getId());
        map.put(cstJsonPluginName, plugIn.getName());
        map.put(cstJsonDescription, description);
        if (mapContent.withExplanation) {
            map.put(cstJsonExplanation, plugIn.getDescription().explanation);
            map.put(cstJsonPlugInDisplayName, plugIn.getDescription().label);
        }
        map.put(cstJsonCron, cronSt);
        map.put(cstJsonHostsRestriction, hostsRestriction);

        // Parameters values
        Map<String, Object> mapParametersValue = new HashMap<>();
        map.put(cstJsonParameters, mapParametersValue);

        // parameter definition
        map.put(CST_JSON_PARAMETERS_DEF, collectParameterList(plugIn.getDefinitionDescription().inputParameters, mapParametersValue));

        // Analysis definition
        map.put(cstJsonAnalysisDef, collectParameterList(plugIn.getDefinitionDescription().analysisParameters, mapParametersValue));

        map.put(cstJsonEnable, isEnable);

        if (mapContent.trackExecution) {
            // todo map.put(cstJsonImmediateExecution, trackExecution.isImmediateExecution);
            map.put("trackExecution", trackExecution.getMap());

        }
        if (mapContent.askStop) {
            map.put(cstJsonAskForStop, trackExecution.askForStop);
        }
        if (mapContent.savedExecution) {
            map.put(cstJsonSavedExec, getListSavedExecution());
        }
        return map;
    }

    private List<Map<String, Object>> collectParameterList(List<PlugInParameter> listPlugInParameters, Map<String, Object> mapParametersValue) {
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        for (PlugInParameter plugInParameter : listPlugInParameters) {

            // complete with the value
            if (plugInParameter.typeParameter == TypeParameter.FILEREAD
                    || plugInParameter.typeParameter == TypeParameter.FILEREADWRITE
                    || plugInParameter.typeParameter == TypeParameter.FILEWRITE) {
                // Attention, in the parameters returned, the FILEWRITE parameters values must be removed
                // Skip this information in fact, will be save in a different way
                mapParametersValue.put(plugInParameter.name, parameters.get(plugInParameter.name + cstPrefixStreamValue) == null ? null : "AVAILABLE");
            } else {
                mapParametersValue.put(plugInParameter.name, parameters.get(plugInParameter.name) == null ? plugInParameter.defaultValue : parameters.get(plugInParameter.name));
            }

            // Parameters Definition
            list.add(plugInParameter.getMap());
        }
        return list;
    }

    public List<Map<String, Object>> getListSavedExecution() {
        // save the last execution
        List<Map<String, Object>> listExecution = new ArrayList<Map<String, Object>>();
        for (SavedExecution savedExecution : listSavedExecution) {
            listExecution.add(savedExecution.getMap());
        }
        return listExecution;
    }

    public String getListSavedExecutionJsonSt() {
        return JSONValue.toJSONString(getListSavedExecution());
    }

    /**
     * serialize in JSON the content of the plugTour
     * 
     * @return
     */
    public String getJsonSt(MapContentParameter mapContent) {
        return JSONValue.toJSONString(getMap(mapContent));
    }

    private void generateId() {
        // sleep a little to be sure to have a unique ID in case of a loop
        try {
            Thread.sleep(2);
        } catch (InterruptedException e) {
        }
        idJob = System.currentTimeMillis();
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Track execution */
    /*                                                                                  */
    /* The execution is managed by the MilkPlugInTourExecution */
    /* But the tracker is manage at the PlugInTour */
    /* - assuming a plugInTour has only one execution in progress */
    /* - The tracker is saved in JSON, and can be read from the database to display */
    /* - Tracker is not directly attached to the MilkPlugInTourExecution thread */
    /* - PlugInTour manage the askForStop() method, when MilkPlugInTourExecution manage */
    /*
     * the pleaseStop() method
     * /*
     */
    /* ******************************************************************************** */
    public class TrackExecution {

        
        public boolean isImmediateExecution() {
            return isImmediateExecution;
        }

        
        public void setImmediateExecution(boolean isImmediateExecution) {
            this.isImmediateExecution = isImmediateExecution;
        }

        
        public boolean isAskForStop() {
            return askForStop;
        }

        
        public void setAskForStop(boolean askForStop) {
            this.askForStop = askForStop;
        }

        
        public Date getNextExecutionDate() {
            return nextExecutionDate;
        }

        
        public void setNextExecutionDate(Date nextExecutionDate) {
            this.nextExecutionDate = nextExecutionDate;
        }

        
        public Date getLastExecutionDate() {
            return lastExecutionDate;
        }

        
        public void setLastExecutionDate(Date lastExecutionDate) {
            this.lastExecutionDate = lastExecutionDate;
        }

        
        public ExecutionStatus getLastExecutionStatus() {
            return lastExecutionStatus;
        }

        
        public void setLastExecutionStatus(ExecutionStatus lastExecutionStatus) {
            this.lastExecutionStatus = lastExecutionStatus;
        }

        
        public boolean isInExecution() {
            return inExecution;
        }

        
        public void setInExecution(boolean inExecution) {
            this.inExecution = inExecution;
        }

        
        public long getStartTime() {
            return startTime;
        }

        
        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        
        public long getPercent() {
            return percent;
        }

        
        public void setPercent(long percent) {
            this.percent = percent;
        }

        
        public long getEndTimeEstimatedInMs() {
            return endTimeEstimatedInMs;
        }

        
        public void setEndTimeEstimatedInMs(long endTimeEstimatedInMs) {
            this.endTimeEstimatedInMs = endTimeEstimatedInMs;
        }

        
        public long getTotalTimeEstimatedInMs() {
            return totalTimeEstimatedInMs;
        }

        
        public void setTotalTimeEstimatedInMs(long totalTimeEstimatedInMs) {
            this.totalTimeEstimatedInMs = totalTimeEstimatedInMs;
        }

        
        public Date getEndDateEstimated() {
            return endDateEstimated;
        }

        
        public void setEndDateEstimated(Date endDateEstimated) {
            this.endDateEstimated = endDateEstimated;
        }

        
        public String getInExecutionHostName() {
            return inExecutionHostName;
        }

        
        public void setInExecutionHostName(String inExecutionHostName) {
            this.inExecutionHostName = inExecutionHostName;
        }

        /**
         * Job will be start at the next heartBeat.
         * See trackExecution attribute to describe an executiong job
         */
        private boolean isImmediateExecution = false;

        public boolean askForStop = false;
        /**
         * keep the last Execution Date and Status, for the dashboard
         */
        public Date nextExecutionDate;
        public Date lastExecutionDate;
        private ExecutionStatus lastExecutionStatus;

        private boolean inExecution = false;
        public long startTime = 0;
        public long percent = 0;
        public long endTimeEstimatedInMs = 0;
        public long totalTimeEstimatedInMs = 0;
        public Date endDateEstimated;

        public String inExecutionHostName;

        public TrackExecution() {
            // nothing special todo here
        };

        public String getHumanTimeEstimated(boolean withMs) {

            return TypesCast.getHumanDuration(endTimeEstimatedInMs, withMs);
        }

        public String getJsonSt() {
            return JSONValue.toJSONString(getMap());
        }

        public Map<String, Object> getMap() {
            Map<String, Object> map = new HashMap<>();

            map.put(cstJsonImmediateExecution, trackExecution.isImmediateExecution);
            map.put(cstJsonAskForStop, trackExecution.askForStop);
            map.put(cstJsonNextExecution, trackExecution.nextExecutionDate == null ? 0 : trackExecution.nextExecutionDate.getTime());
            map.put(cstJsonNextExecution + cstJsonPrefixHumanReadable, trackExecution.nextExecutionDate == null ? "" : sdf.format(trackExecution.nextExecutionDate.getTime()));
            map.put(cstJsonLastExecution, trackExecution.lastExecutionDate == null ? 0 : trackExecution.lastExecutionDate.getTime());
            map.put(cstJsonLastExecution + cstJsonPrefixHumanReadable, trackExecution.lastExecutionDate == null ? "" : sdf.format(trackExecution.lastExecutionDate));
            String executionStatus = trackExecution.lastExecutionStatus == null ? null : trackExecution.lastExecutionStatus.toString().toLowerCase();
            map.put(cstJsonlastExecutionStatus, executionStatus);

            map.put(cstJsonInExecution, trackExecution.inExecution);
            map.put(cstJsonInExecutionStartTime, trackExecution.startTime);
            map.put(cstJsonInExecutionPercent, trackExecution.percent);
            map.put(cstJsonInExecutionEndTimeEstimatedInMS, trackExecution.endTimeEstimatedInMs);
            map.put(cstJsonInExecutionEndTimeEstimatedInMS + cstJsonPrefixHumanReadable, trackExecution.getHumanTimeEstimated(false));
            map.put(cstJsonInExecutionEndDateEstimated + cstJsonPrefixHumanReadable, trackExecution.endDateEstimated == null ? "" : sdf.format(trackExecution.endDateEstimated));
            map.put(cstJsonRegisterInExecutionHost, trackExecution.inExecutionHostName);
            return map;
        }

        @SuppressWarnings("unchecked")
        public void readFromJson(String jsonSt) {
            Map<String, Object> jsonMap = (Map<String, Object>) JSONValue.parse(jsonSt);
            isImmediateExecution = getBooleanValue(jsonMap.get(cstJsonImmediateExecution), false);
            askForStop = getBooleanValue(jsonMap.get(cstJsonAskForStop), false);

            Long nextExecutionDateLong = (Long) jsonMap.get(cstJsonNextExecution);
            if (nextExecutionDateLong != null && nextExecutionDateLong != 0)
                trackExecution.nextExecutionDate = new Date(nextExecutionDateLong);

            Long lastExecutionDateLong = (Long) jsonMap.get(cstJsonLastExecution);
            if (lastExecutionDateLong != null && lastExecutionDateLong != 0)
                trackExecution.lastExecutionDate = new Date(lastExecutionDateLong);

            String lastExecutionStatus = (String) jsonMap.get(cstJsonlastExecutionStatus);
            if (lastExecutionStatus != null)
                trackExecution.lastExecutionStatus = ExecutionStatus.valueOf(lastExecutionStatus.toUpperCase());

            inExecution = getBooleanValue(jsonMap.get(cstJsonInExecution), false);
            startTime = getLongValue(jsonMap.get(cstJsonInExecutionStartTime), 0L);
            percent = getLongValue(jsonMap.get(cstJsonInExecutionPercent), 0L);
            endTimeEstimatedInMs = getLongValue(jsonMap.get(cstJsonInExecutionEndTimeEstimatedInMS), 0L);
            endDateEstimated = null;
            if (startTime > 0 && endTimeEstimatedInMs > 0) {
                endDateEstimated = new Date(startTime + endTimeEstimatedInMs);
            }
            inExecutionHostName = (String) jsonMap.get(cstJsonRegisterInExecutionHost);
        }

    }

    public TrackExecution trackExecution = new TrackExecution();

    /**
     * reload information for the database: user can ask a stop
     * 
     * @return
     */
    public boolean isAskedForStop() {
        // load on the database
        MilkFactoryOp jobLoaded = milkJobFactory.dbLoadJob(idJob);
        trackExecution.askForStop = jobLoaded.job.trackExecution.askForStop;
        return trackExecution.askForStop;
    }

    public void setAskForStop(boolean askForStop) {
        this.trackExecution.askForStop = askForStop;
    }

    
    public boolean inExecution() {
        return trackExecution.inExecution;
    }

    public void setInExecution( boolean inExecution ) {
        if (trackExecution != null)
            trackExecution.inExecution = inExecution;
    }
    public void registerExecutionOnHost(String hostName) {
        trackExecution.inExecutionHostName = hostName;
    }

    public String getHostRegistered() {
        return trackExecution.inExecutionHostName;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Saved execution */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    private static class SavedExecution {

        public Date executionDate;
        public ExecutionStatus executionStatus;

        private String listEventSt;
        public String explanation;
        public long nbItemsProcessed = 0;
        public long executionTimeInMs;
        public String hostName;

        public SavedExecution() {
        };

        public SavedExecution(PlugTourOutput output) {
            executionDate = output.executionDate;
            executionStatus = output.executionStatus;
            listEventSt = BEventFactory.getSyntheticHtml(output.getListEvents());
            explanation = output.explanation;
            nbItemsProcessed = output.nbItemsProcessed;
            executionTimeInMs = output.executionTimeInMs;
            hostName = output.hostName;
        }

        public Map<String, Object> getMap() {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put(cstJsonSaveExecDate, executionDate.getTime());
            map.put(cstJsonSaveExecDate + cstJsonPrefixHumanReadable, sdf.format(executionDate));
            map.put(cstJsonSaveExecStatus, executionStatus.toString());
            map.put(cstJsonSaveExecListEventsSt, listEventSt);
            map.put(cstJsonSaveExecExplanation, explanation);
            map.put(cstJsonSaveExecItemsProcessed, nbItemsProcessed);
            map.put(cstJsonSaveExecTimeinMs, executionTimeInMs);
            map.put(cstJsonSaveExecTimeinMs + cstJsonPrefixHumanReadable, TypesCast.getHumanDuration(executionTimeInMs, false));

            map.put(cstJsonSaveExecHostName, hostName);
            return map;
        }

        public static SavedExecution getInstance(Map<String, Object> jsonMap) {
            SavedExecution savedExecution = new SavedExecution();

            Long execDateLong = (Long) jsonMap.get(cstJsonSaveExecDate);
            if (execDateLong != null && execDateLong != 0)
                savedExecution.executionDate = new Date(execDateLong);
            try {
                savedExecution.executionStatus = ExecutionStatus.valueOf((String) jsonMap.get(cstJsonSaveExecStatus));
            } catch (Exception e) {
                // anormal
                savedExecution.executionStatus = ExecutionStatus.ERROR;
            }
            savedExecution.listEventSt = (String) jsonMap.get(cstJsonSaveExecListEventsSt);
            savedExecution.explanation = (String) jsonMap.get(cstJsonSaveExecExplanation);
            savedExecution.nbItemsProcessed = (Long) jsonMap.get(cstJsonSaveExecItemsProcessed);
            savedExecution.executionTimeInMs = (Long) jsonMap.get(cstJsonSaveExecTimeinMs);
            savedExecution.hostName = (String) jsonMap.get(cstJsonSaveExecHostName);
            return savedExecution;
        }
    }

    // save a tour execution
    List<SavedExecution> listSavedExecution = new ArrayList<SavedExecution>();

    @SuppressWarnings("unchecked")
    public void readSavedExecutionFromJson(String jsonSt) {
        List<Map<String, Object>> jsonList = (List<Map<String, Object>>) JSONValue.parse(jsonSt);
        if (jsonList != null) {
            for (Map<String, Object> execSaveMap : jsonList) {
                listSavedExecution.add(SavedExecution.getInstance(execSaveMap));
            }
        }
    }

    /**
     * register an execution. Keep the last 10
     * 
     * @param currentDate
     * @param output
     */
    public void registerExecution(Date currentDate, PlugTourOutput output) {
        trackExecution.lastExecutionDate = output.executionDate;
        trackExecution.lastExecutionStatus = output.executionStatus;
        if (output.executionStatus == ExecutionStatus.SUCCESSNOTHING
                || output.executionStatus == ExecutionStatus.NOEXECUTION) {
            return; // no need to save it
        }

        SavedExecution savedExecution = new SavedExecution(output);
        listSavedExecution.add(0, savedExecution);
        if (listSavedExecution.size() > 10)
            listSavedExecution.remove(10);
    }
}
