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
import java.util.logging.Logger;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.ExecutionStatus;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugTourOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugInFactory;
import org.bonitasoft.truckmilk.engine.MilkJobFactory;
import org.bonitasoft.truckmilk.engine.MilkJobFactory.MilkFactoryOp;
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

    public static Logger logger = Logger.getLogger(MilkJob.class.getName());
    public static String logHeader = "MilkPlugInTour";

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
    public long idJob = 0;
    /** keep the mark, then we can save again immediately if the tour didn't have an ID */
    public boolean newIdGenerated = false;

    public String name;
    public String description;

    public boolean isEnable = false;

    /**
     * Job will be start at the next heartBeat.
     * See trackExecution attribute to describe an executiong job
     */
    public boolean isImmediateExecution = false;

    public String cronSt = "";
    public Date nextExecutionDate;

    /** list of host name or ip address separate by a ;
    public String hostRestriction;
    /**
     * in a Cluster environment, we may want this plugInTour is executed only on a specific node.
     */
    public String hostsRestriction = null;
    /**
     * keep the last Execution Date and Status, for the dashboard
     */
    public Date lastExecutionDate;
    public ExecutionStatus lastExecutionStatus;

    /**
     * save the Value for all parameters, even STREAM parameters (end with "_st")
     */
    public Map<String, Object> parameters = new HashMap<String, Object>();

    public MilkJob(String name, MilkPlugIn plugIn, MilkJobFactory milkJobFactory) {
        this.plugIn = plugIn;
        this.name = name == null ? MilkJob.DEFAULT_NAME : name;
        this.milkJobFactory =  milkJobFactory;
    }

    public static MilkJob getInstanceFromPlugin(String name, MilkPlugIn plugIn,MilkJobFactory milkPlugInTourFactory) {
        MilkJob milkPlugInTour = new MilkJob(name, plugIn, milkPlugInTourFactory);
        PlugInDescription description = plugIn.getDescription();
        // clone the parameters !
        // new HashMap<>(description.getParametersMap()) not need at this moment because the maps is created
        milkPlugInTour.parameters = description.getParametersMap();
        milkPlugInTour.cronSt = description.cronSt;
        milkPlugInTour.name = name;
        // generate an ID
        milkPlugInTour.checkId();
        return milkPlugInTour;
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
     * execute a tour
     * /*
     */
    /* ******************************************************************************** */

    public List<BEvent> calculateNextExecution() {
        List<BEvent> listEvents = new ArrayList<BEvent>();

        try {
            CronExpression cronExp = new CronExpression(cronSt);
            nextExecutionDate = cronExp.getNextValidTimeAfter(new Date());
        } catch (Exception e) {
            nextExecutionDate = null;
            listEvents.add(new BEvent(eventCronParseError, e, "Expression[" + cronSt + "]"));
        }
        return listEvents;
    }

    public List<BEvent> setCron(String cronSt) {
        this.cronSt = cronSt;
        return calculateNextExecution();
    }
    public void setHostsRestriction(String hostsRestriction )
    {
        this.hostsRestriction = hostsRestriction;
    }
    public void setDescription(String description) {
        this.description = description;
    }

    public Date getNextExecution() {
        return nextExecutionDate;
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
            nextExecutionDate = null;
        return listEvents;
    }

    /**
     * Next check ? Start immediately
     */
    public void setImmediateExecution(boolean immediateExecution) {
        isImmediateExecution = immediateExecution;

    }

    public boolean isImmediateExecution() {
        return isImmediateExecution;
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
    public void setTourParameters(Map<String, Object> parameters) {
        this.parameters = parameters == null ? new HashMap<String, Object>() : parameters;
    }

    /**
     * set a Stream parameters
     * 
     * @param parameterName
     * @param temporaryFileName
     * @param pageDirectory
     */
    public List<BEvent> setTourFileParameter(String parameterName, String temporaryFileName, File pageDirectory) {
        List<BEvent> listEvents = new ArrayList<BEvent>();
        List<String> listParentTmpFile = new ArrayList<String>();
        try {
            listParentTmpFile.add(pageDirectory.getCanonicalPath() + "/../../../tmp/");
            listParentTmpFile.add(pageDirectory.getCanonicalPath() + "/../../");
        } catch (Exception e) {
            logger.info(logHeader + ".setTourFileParameter: error get CanonicalPath of pageDirectory[" + e.toString() + "]");
            listEvents.add(EVENT_CANT_FIND_TEMPORARY_PATH);
            return listEvents;
        }
        String detectedPaths = "";
        boolean findFile = false;
        for (String pathTemp : listParentTmpFile) {
            // logger.fine(logHeader+".setTourFileParameter: CompleteuploadFile  TEST [" + pathTemp + temporaryFileName + "]");
            detectedPaths += "[" + pathTemp + temporaryFileName + "],";
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
                    logger.info(logHeader + ".setTourFileParameter: File[" + pathTemp + temporaryFileName + "] Exception " + e.getMessage());
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
 * @return
 */
    public boolean isInsideHostsRestriction() {
        if (hostsRestriction == null)
            return true; // no limitation

        String compareHostRestriction = ";"+hostsRestriction+";";
        try {
            InetAddress ip = InetAddress.getLocalHost();

            if (compareHostRestriction.indexOf(";"+ip.getHostAddress()+";")!= -1)
                return true;
            if (compareHostRestriction.indexOf(";"+ip.getHostName()+";")!= -1)
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
            logger.severe(logHeader + ".getParameterStream:Error writing parameter " + e.getMessage());
        }
        return listEvents;
    }

    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * describe the plug tour
     * 
     * @return
     */
    private final static String cstJsonPluginName = "pluginname";
    private final static String cstJsonEnable = "enable";

    private final static String cstJsonParametersDef = "parametersdef";
    private final static String cstJsonParameters = "parameters";
    // private final static String cstJsonParameterValue = "value";

    private final static String cstJsonPlugInDisplayName = "plugindisplayname";
    private final static String cstJsonDescription = "description";
    private final static String cstJsonExplanation = "explanation";
    public final static String cstJsonCron = "cron";
    public final static String cstJsonHostsRestriction = "hostsrestriction";
    private final static String cstJsonNextExecution = "nextexecution";
    private final static String cstJsonLastExecution = "lastexecution";
    private final static String cstJsonName = "name";
    private final static String cstJsonId = "id";
    private final static String cstJsonImmediateExecution = "imediateExecution";
    private final static String cstJsonAskForStop = "askForStop";
    
    private final static String cstJsonlastExecutionStatus = "lastexecutionstatus";
    private final static String cstJsonInExecution = "inExecution";

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

    /**
     * getInstanceFromMap (the load)
     * @see getMap()
     * 
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
        MilkJob plugInTour = new MilkJob(name, plugIn, milkPlugInTourFactory);
        plugInTour.description = (String) jsonMap.get(cstJsonDescription);

        plugInTour.idJob = getLongValue(jsonMap.get(cstJsonId), 0L);
        plugInTour.checkId();

        // clone the parameters !
        // new HashMap<>(description.getParametersMap()) not need at this moment because the maps is created
        plugInTour.parameters = (Map<String, Object>) jsonMap.get(cstJsonParameters);

        @SuppressWarnings("unused")
        List<Map<String, Object>> listParametersDef = (List<Map<String, Object>>) jsonMap.get(cstJsonParametersDef);

        plugInTour.cronSt = (String) jsonMap.get(cstJsonCron);
        plugInTour.hostsRestriction = (String) jsonMap.get(cstJsonHostsRestriction);
        
        // search the name if all the list
        plugInTour.isEnable = getBooleanValue(jsonMap.get(cstJsonEnable), false);
        plugInTour.isImmediateExecution = getBooleanValue(jsonMap.get(cstJsonImmediateExecution), false);
        plugInTour.askForStop = getBooleanValue(jsonMap.get(cstJsonAskForStop), false); 
        
        Long nextExecutionDateLong = (Long) jsonMap.get(cstJsonNextExecution);
        if (nextExecutionDateLong != null && nextExecutionDateLong != 0)
            plugInTour.nextExecutionDate = new Date(nextExecutionDateLong);

        Long lastExecutionDateLong = (Long) jsonMap.get(cstJsonLastExecution);
        if (lastExecutionDateLong != null && lastExecutionDateLong != 0)
            plugInTour.lastExecutionDate = new Date(lastExecutionDateLong);

        String lastExecutionStatus = (String) jsonMap.get(cstJsonlastExecutionStatus);
        if (lastExecutionStatus != null)
            plugInTour.lastExecutionStatus = ExecutionStatus.valueOf(lastExecutionStatus.toUpperCase());

        plugInTour.trackExecution.inExecution = getBooleanValue(jsonMap.get(cstJsonInExecution), false);
        plugInTour.trackExecution.startTime = getLongValue(jsonMap.get(cstJsonInExecutionStartTime), 0L);
        plugInTour.trackExecution.percent = getLongValue(jsonMap.get(cstJsonInExecutionPercent), 0L);
        plugInTour.trackExecution.timeEstimatedInMs = getLongValue(jsonMap.get(cstJsonInExecutionEndTimeEstimatedInMS), 0L);
        plugInTour.trackExecution.endDateEstimated = null;
        if (plugInTour.trackExecution.startTime>0 && plugInTour.trackExecution.timeEstimatedInMs>0)
        {
            plugInTour.trackExecution.endDateEstimated= new Date(plugInTour.trackExecution.startTime + plugInTour.trackExecution.timeEstimatedInMs);
        }
        
        if (plugInTour.isEnable && plugInTour.nextExecutionDate == null)
            plugInTour.calculateNextExecution();

        // get the last saved execution
        List<Map<String, Object>> list = (List<Map<String, Object>>) jsonMap.get(cstJsonSavedExec);
        if (list != null) {
            for (Map<String, Object> execSaveMap : list) {
                plugInTour.listSavedExecution.add(SavedExecution.getInstance(execSaveMap));
            }
        }
        return plugInTour;
    }

    /**
     * copy a plugInSource from an another. ID is not copyed by this way, suppose to be the same.
     * 
     * @param plugInTourSource
     * @param milkCmdControl
     */
    public void copyFrom(MilkJob plugInTourSource, MilkJobFactory milkPlugInTourFactory) {
        name = plugInTourSource.name;
        description = plugInTourSource.description;
        // ID does not change
        parameters = plugInTourSource.parameters;

        cronSt = plugInTourSource.cronSt;
        isEnable = plugInTourSource.isEnable;
        isImmediateExecution = plugInTourSource.isImmediateExecution;
        nextExecutionDate = plugInTourSource.nextExecutionDate;
        lastExecutionDate = plugInTourSource.lastExecutionDate;
        lastExecutionStatus = plugInTourSource.lastExecutionStatus;
        listSavedExecution = plugInTourSource.listSavedExecution;
        return;
    }

    /**
     * getMap : use to save it or send to the browser.
     * browser need to have the parameters definition AND the value, save need only the parameter
     * value.
     * 
     * @see getInstanceFromJson
     * @param withExplanation
     * @return
     */
    public Map<String, Object> getMap(boolean withExplanation) {

        Map<String, Object> map = new HashMap<String, Object>();
        map.put(cstJsonName, getName());
        map.put(cstJsonId, getId());
        map.put(cstJsonPluginName, plugIn.getName());
        map.put(cstJsonDescription, description);
        if (withExplanation) {
            map.put(cstJsonExplanation, plugIn.getDescription().explanation);
            map.put(cstJsonPlugInDisplayName, plugIn.getDescription().label);
        }
        map.put(cstJsonCron, cronSt);
        map.put(cstJsonHostsRestriction, hostsRestriction);
        

        // Parameters values
        Map<String, Object> mapParametersValue = new HashMap<String, Object>();
        map.put(cstJsonParameters, mapParametersValue);

        // parameter definition
        List<Map<String, Object>> listParametersDef = new ArrayList<Map<String, Object>>();
        map.put(cstJsonParametersDef, listParametersDef);

        for (PlugInParameter plugInParameter : plugIn.getDefinitionDescription().inputParameters) {

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
            listParametersDef.add(plugInParameter.getMap());
        }

        if (isEnable) {
            map.put(cstJsonNextExecution, nextExecutionDate == null ? 0 : nextExecutionDate.getTime());
            map.put("nextexecutionst", nextExecutionDate == null ? "" : sdf.format(nextExecutionDate));
        }

        map.put(cstJsonLastExecution, lastExecutionDate == null ? 0 : lastExecutionDate.getTime());
        map.put("lastexecutionst", lastExecutionDate == null ? "" : sdf.format(lastExecutionDate));
        String executionStatus = lastExecutionStatus == null ? null : lastExecutionStatus.toString().toLowerCase();

        map.put(cstJsonlastExecutionStatus, executionStatus);
        map.put(cstJsonInExecution, trackExecution.inExecution);
        map.put(cstJsonInExecutionStartTime, trackExecution.startTime);
        map.put(cstJsonInExecutionPercent, trackExecution.percent);
        map.put(cstJsonInExecutionEndTimeEstimatedInMS, trackExecution.timeEstimatedInMs);
        map.put(cstJsonInExecutionEndTimeEstimatedInMS+"st", trackExecution.getHumanTimeEstimated( false ));
        map.put(cstJsonInExecutionEndDateEstimated + "st", trackExecution.endDateEstimated==null ? "" : sdf.format(trackExecution.endDateEstimated));

        map.put(cstJsonEnable, isEnable);
        map.put(cstJsonImmediateExecution, isImmediateExecution);
        map.put(cstJsonAskForStop, askForStop);

        // save the last execution
        List<Map<String, Object>> listExecution = new ArrayList<Map<String, Object>>();
        for (SavedExecution savedExecution : listSavedExecution) {
            listExecution.add(savedExecution.getMap());
        }
        map.put(cstJsonSavedExec, listExecution);

        return map;
    }

    /**
     * serialize in JSON the content of the plugTour
     * 
     * @return
     */
    public String getJsonSt() {
        return JSONValue.toJSONString(getMap(false));
    }

    private void checkId() {
        if (idJob == 0) {
            // sleep a little to be sure to have a unique ID in case of a loop
            newIdGenerated = true;
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
            }
            idJob = System.currentTimeMillis();
        }
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

        public boolean inExecution = false;
        public long startTime = 0;
        public long percent = 0;
        public long timeEstimatedInMs=0;
        public Date endDateEstimated;
        public String getHumanTimeEstimated( boolean withMs)
        {
            String result="";
            long timeRes= timeEstimatedInMs;
            long nbDays= timeRes / (1000*60*60*24);
            if ( nbDays > 1)
                result+= nbDays+" days ";
            timeRes = timeRes - nbDays*(1000*60*60*24);
              
            long nbHours= timeRes / (1000*60*60);
            result += String.format("%02d", nbHours)+":";
            timeRes = timeRes - nbHours*(1000*60*60);

            long nbMinutes= timeRes / (1000*60);
            result += String.format("%02d", nbMinutes)+":";
            timeRes = timeRes - nbMinutes*(1000*60);

            long nbSecond= timeRes / (1000);
            result += String.format("%02d", nbSecond)+" ";
            timeRes = timeRes - nbSecond*(1000);
            
            if (withMs)
                result += String.format("%03d", timeRes);
            
            return result;
        }
    }

    public TrackExecution trackExecution = new TrackExecution();

    public boolean askForStop = false;

    public boolean askForStop() {
        // load on the database
        MilkFactoryOp jobLoaded = milkJobFactory.dbLoadJob( idJob );
        this.askForStop = jobLoaded.job.askForStop;
        return askForStop;
    }

    public void setAskForStop(boolean askForStop) {
        this.askForStop = askForStop;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Save execution */
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

        public SavedExecution() {
        };

        public SavedExecution(PlugTourOutput output) {
            executionDate = output.executionDate;
            executionStatus = output.executionStatus;
            listEventSt = BEventFactory.getSyntheticHtml(output.getListEvents());
            explanation = output.explanation;
            nbItemsProcessed = output.nbItemsProcessed;
            executionTimeInMs = output.executionTimeInMs;
        }

        public Map<String, Object> getMap() {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put(cstJsonSaveExecDate, executionDate.getTime());
            map.put(cstJsonSaveExecDate + "St", sdf.format(executionDate));
            map.put(cstJsonSaveExecStatus, executionStatus.toString());
            map.put(cstJsonSaveExecListEventsSt, listEventSt);
            map.put(cstJsonSaveExecExplanation, explanation);
            map.put(cstJsonSaveExecItemsProcessed, nbItemsProcessed);
            map.put(cstJsonSaveExecTimeinMs, executionTimeInMs);

            return map;
        }

        public static SavedExecution getInstance(Map<String, Object> jsonMap) {
            SavedExecution savedExecution = new SavedExecution();

            Long execDateLong = (Long) jsonMap.get(cstJsonSaveExecDate);
            if (execDateLong != null && execDateLong != 0)
                savedExecution.executionDate = new Date(execDateLong);
            savedExecution.executionStatus = ExecutionStatus.valueOf((String) jsonMap.get(cstJsonSaveExecStatus));
            savedExecution.listEventSt = (String) jsonMap.get(cstJsonSaveExecListEventsSt);
            savedExecution.explanation = (String) jsonMap.get(cstJsonSaveExecExplanation);
            savedExecution.nbItemsProcessed = (Long) jsonMap.get(cstJsonSaveExecItemsProcessed);
            savedExecution.executionTimeInMs = (Long) jsonMap.get(cstJsonSaveExecTimeinMs);

            return savedExecution;
        }
    }

    // save a tour execution
    List<SavedExecution> listSavedExecution = new ArrayList<SavedExecution>();

    /**
     * register an execution. Keep the last 10
     * 
     * @param currentDate
     * @param output
     */
    public void registerExecution(Date currentDate, PlugTourOutput output) {
        lastExecutionDate = output.executionDate;
        lastExecutionStatus = output.executionStatus;
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
