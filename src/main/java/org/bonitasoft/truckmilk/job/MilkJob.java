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
import java.util.Map.Entry;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.engine.MilkJobFactory;
import org.bonitasoft.truckmilk.engine.MilkJobFactory.MilkFactoryOp;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInMesure;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugInFactory;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.DelayResult;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;

import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.toolbox.MilkLog;
import org.bonitasoft.truckmilk.toolbox.TypesCast;
import org.quartz.CronExpression;

import lombok.Data;

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

public @Data class MilkJob {

    private final static MilkLog logger = MilkLog.getLogger(MilkJob.class.getName());
    private final static String LOGGER_HEADER = "MilkJob ";
    private final static SimpleDateFormat sdfSynthetic = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * In case of limitation, this is the number of data
     */
    public final static int CSTMAXLIMITEDDATA = 20;

    private static BEvent eventCronParseError = new BEvent(MilkJob.class.getName(), 1, Level.APPLICATIONERROR,
            "Bad cron expression ", "Cron expression is not correct", "The next date can't be calculated",
            "Give a correct date");

    private static BEvent eventErrorReadingParamFile = new BEvent(MilkJob.class.getName(), 3, Level.APPLICATIONERROR,
            "Error reading Parameter File", "An error occure during the reading of a parameters file", "Content can't be accessible",
            "Check the log");

    private static BEvent eventCantFindTemporaryPath = new BEvent(MilkJob.class.getName(), 4, Level.ERROR,
            "Can't find temporary path", "File are uploaded in a temporary path. This path can't be found", "Content will not be updated",
            "Check the temporary path");

    private static BEvent eventCantFindTemporaryFile = new BEvent(MilkJob.class.getName(), 5, Level.ERROR,
            "Can't find temporary file", "File are uploaded in a temporary path. File is not found in the temporary path", "Content will not be updated",
            "Check the temporary path; the log. Maybe file is full?");

    public final static String DEFAULT_NAME = "";
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

    private boolean isEnable = false;

    public String cronSt = "";

    public int nbSavedExecution;
    public int nbHistoryMesure;

    public int stopAfterNbItems;
    public int stopAfterNbMinutes;

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
    public Map<String, Object> parameters = new HashMap<>();

    public enum ExecutionStatus {
        /**
         * an execution is in progress.
         * THis state is not used too much, because the enum is used to save the last execution status, not the current one
         */
        EXECUTING,
        /**
         * No execution is performing
         */
        NOEXECUTION,
        /**
         * One execution is done with success
         */
        SUCCESS,
        /**
         * One execution is done with success, but nothing was processed
         */
        SUCCESSNOTHING,
        /**
         * execution is done with success, but stop before the end (Job detect it has still something to do)
         */
        SUCCESSPARTIAL,
        /**
         * execution is done with success, but stop before the end (askStop)
         */
        SUCCESSABORT,
        /**
         * execution is done with success, but stop before the end (askStop)
         */
        KILL,
        /**
         * use Warning when needed. Not an error, but something not going good
         */
        WARNING,
        /**
         * Error arrived in the execution
         */
        ERROR,
        /**
         * The plug in does not respect the contract of execution
         */
        CONTRACTVIOLATION,
        /**
         * Something is wrong in the configuration, and the plugin can't run
         */
        BADCONFIGURATION
    }

    public MilkJob(String name, MilkPlugIn plugIn, MilkJobFactory milkJobFactory) {
        this.plugIn = plugIn;
        this.name = name == null ? MilkJob.DEFAULT_NAME : name;
        this.milkJobFactory = milkJobFactory;

        this.generateId();
    }

    public static MilkJob getInstanceFromPlugin(String name, MilkPlugIn plugIn, MilkJobFactory milkPlugInTourFactory) {
        MilkJob milkJob = new MilkJob(name, plugIn, milkPlugInTourFactory);
        MilkPlugInDescription description = plugIn.getDescription();
        // clone the parameters !
        // new HashMap<>(description.getParametersMap()) not need at this moment because the maps is created
        milkJob.parameters = description.getParametersMap();
        milkJob.cronSt = description.getCronSt();
        milkJob.nbSavedExecution = plugIn.getDefaultNbSavedExecution();
        milkJob.nbHistoryMesure = plugIn.getDefaultNbHistoryMesures();

        return milkJob;
    }

    public String toString() {
        return name + " (" + (isEnable ? "ENABLE " : "disable ")
                + (trackExecution.isImmediateExecution ? "<i> " : "")
                + (trackExecution.inExecution ? "EXECUTION_IN_PROGRESS " : "")
                + (trackExecution.nextExecutionDate == null ? "NoNextDate" : sdf.format(trackExecution.nextExecutionDate)) + ")";
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
        List<BEvent> listEvents = new ArrayList<>();
        if (parameters == null)
            parameters = new HashMap<>();
        // verify that all plugin parameters are in
        for (PlugInParameter plugInParameter : plugIn.getDescription().getInputParameters()) {
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

    /**
     * @param origin origin of caller
     * @return
     */

    public List<BEvent> calculateNextExecution(String origin) {
        List<BEvent> listEvents = new ArrayList<>();
        logger.fine(LOGGER_HEADER + "Calculate Next Execution[" + toString() + "] by [" + origin + "]");
        try {
            CronExpression cronExp = new CronExpression(cronSt);
            setNextExecutionDate(cronExp.getNextValidTimeAfter(new Date()));
        } catch (Exception e) {
            setNextExecutionDate(null);
            listEvents.add(new BEvent(eventCronParseError, e, "Expression[" + cronSt + "]"));
        }
        return listEvents;
    }

    public List<BEvent> setCron(String cronSt) {
        this.cronSt = cronSt;
        return calculateNextExecution("ChangeCronString");
    }

    public void setHostsRestriction(String hostsRestriction) {
        this.hostsRestriction = hostsRestriction;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /*
     * Tracking execution a job
     * /*
     */
    /* ******************************************************************************** */
    private TrackExecution trackExecution = new TrackExecution();

    public Date getNextExecutionDate() {
        return trackExecution.nextExecutionDate;
    }

    public String getNextExecutionDateSt() {
        return (trackExecution.nextExecutionDate == null ? "null" : sdfSynthetic.format(trackExecution.nextExecutionDate));
    }

    public void setNextExecutionDate(Date nextExecutionDate) {
        logger.fine(LOGGER_HEADER + "Job[" + name + "] (enable:" + isEnable + ") Set Execution Date [" + (nextExecutionDate == null ? "null" : sdf.format(nextExecutionDate)) + "]");
        this.trackExecution.nextExecutionDate = nextExecutionDate;
    }

    public boolean isAskForStop() {
        return this.trackExecution.askForStop;
    }

    public void killJob() {
        setLastExecutionDate(new Date());
        setLastExecutionStatus(ExecutionStatus.KILL);
        setImmediateExecution(false);
        setInExecution(false);
    }

    public Date getLastExecutionDate() {
        return this.trackExecution.lastExecutionDate;
    }

    public void setLastExecutionDate(Date lastExecutionDate) {
        this.trackExecution.lastExecutionDate = lastExecutionDate;
    }

    public ExecutionStatus getLastExecutionStatus() {
        return this.trackExecution.lastExecutionStatus;
    }

    public void setLastExecutionStatus(ExecutionStatus lastExecutionStatus) {
        this.trackExecution.lastExecutionStatus = lastExecutionStatus;
    }

    public boolean isInExecution() {
        return this.trackExecution.inExecution;
    }

    public long getStartTime() {
        return this.trackExecution.startTime;
    }

    public void setStartTime(long startTime) {
        this.trackExecution.startTime = startTime;
    }

    public long getPercent() {
        return this.trackExecution.percent;
    }

    public void setPercent(long percent) {
        this.trackExecution.percent = percent;
    }

    public long getEndTimeEstimatedInMs() {
        return this.trackExecution.endTimeEstimatedInMs;
    }

    public void setEndTimeEstimatedInMs(long endTimeEstimatedInMs) {
        this.trackExecution.endTimeEstimatedInMs = endTimeEstimatedInMs;
    }

    public long getTotalTimeEstimatedInMs() {
        return this.trackExecution.totalTimeEstimatedInMs;
    }

    public void setTotalTimeEstimatedInMs(long totalTimeEstimatedInMs) {
        this.trackExecution.totalTimeEstimatedInMs = totalTimeEstimatedInMs;
    }

    public Date getEndDateEstimated() {
        return this.trackExecution.endDateEstimated;
    }

    public void setEndDateEstimated(Date endDateEstimated) {
        this.trackExecution.endDateEstimated = endDateEstimated;
    }

    public String getInExecutionHostName() {
        return this.trackExecution.inExecutionHostName;
    }

    public void setInExecutionHostName(String inExecutionHostName) {
        this.trackExecution.inExecutionHostName = inExecutionHostName;
    }

    public String getHumanTimeEstimated(boolean withMs) {

        return TypesCast.getHumanDuration(this.trackExecution.endTimeEstimatedInMs, withMs);
    }

    /**
     * set the status. If enaablme, then the next execution is calculated according the cronSt given
     * 
     * @param enable
     */
    public List<BEvent> setEnable(boolean enable, MilkJobExecution milkJobExecution) {
        List<BEvent> listEvents = new ArrayList<>();
        if (enable) {
            // recalculate only if it wasn't enable before
            if (!isEnable)
                listEvents.addAll(calculateNextExecution("Switch To Enable=true"));
        } else
            trackExecution.nextExecutionDate = null;

        if (enable)
            getPlugIn().notifyActivateAJob(this, milkJobExecution);
        else
            getPlugIn().notifyUnactivateAJob(this, milkJobExecution);
        
        isEnable = enable;
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

    public void setInExecution(boolean inExecution) {
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
    /*
     * Mesure
     * /*
     */
    /* ******************************************************************************** */
    public static class MesureAtDate {

        public Date date;
        public Map<PlugInMesure, Double> values;
        public MesureAtDate(Date date ) {
            this.date = date;
        }
    }

    private List<MesureAtDate> listAllMesureValues = new ArrayList<>();

    public void addMesureValues(Date date, Map<PlugInMesure, Double> values) {
        MesureAtDate mesure = new MesureAtDate( date );
        mesure.values = values;
        listAllMesureValues.add(mesure);
        if (listAllMesureValues.size() > getNbHistoryMesure()) {
            int starter = listAllMesureValues.size() - getNbHistoryMesure();
            listAllMesureValues = listAllMesureValues.subList(starter, listAllMesureValues.size());
        }
    }

    /**
     * for serialization
     *  listOfValues is true : then the result is a list of value, following the plugInDescription (usefull for the display)
     *           [ {"date": Date , "valueslist" :  [12.4,43 ]}]
 
     *  false : a map <MesureName, mesureValue> : better for the serialization, when a new mesure can be add between two saves.
     *            [ {"date": Date , "values" : {"Temperature": 12.4", "Weight":43]
  
     * 
     * @return
     */
    public List<Map<String, Object>> getListMesureValues( boolean listOfValues) {
        List<Map<String, Object>> listPoints = new ArrayList<>();
        for (MesureAtDate mesureAtDate : listAllMesureValues) {
            Map<String, Object> mapMesureValue = new HashMap<>();
            mapMesureValue.put( CSTJSON_MESUREATDATE_DATEST, mesureAtDate.date==null ? "" : sdf.format(mesureAtDate.date));
            if (listOfValues) {
                List<Double> listValues = new ArrayList<>();
                for (PlugInMesure plugInMesure : plugIn.getDescription().getListMesures()) {
                    listValues.add( mesureAtDate.values.get(plugInMesure));
                }
                mapMesureValue.put(CSTJSON_MESUREATDATE_VALUELIST, listValues);
            } else {
                Map<String, Double> mapValues = new HashMap<>();
                for (Entry<PlugInMesure, Double> entry :  mesureAtDate.values.entrySet()) {
                    mapValues.put(entry.getKey().getName(), entry.getValue());
                }
                mapMesureValue.put(CSTJSON_MESUREATDATE_VALUEMAP, mapValues);

            }
            listPoints.add(mapMesureValue);
        }
        return listPoints;
    }

    /**
     * for serialization
     * 
     * @return
     */
    @SuppressWarnings("unchecked")
    public void readMesureValuesFromList(List<Map<String, Object>> listPoints) {
        listAllMesureValues = new ArrayList<>();
        for (Map<String, Object> pointMap : listPoints) {
            try {
                Date dateMesure=  sdf.parse((String) pointMap.get( CSTJSON_MESUREATDATE_DATEST ));
                MesureAtDate mesureAtDate = new MesureAtDate(dateMesure);

                mesureAtDate.values = new HashMap<>();

                for (Entry<String, Double> entry : ((Map<String, Double>) pointMap.get(CSTJSON_MESUREATDATE_VALUEMAP)).entrySet()) {
                    PlugInMesure mesure = getPlugIn().getDescription().getMesureFromName(entry.getKey());
                    if (mesure != null)
                        mesureAtDate.values.put(mesure, entry.getValue());
                }
                listAllMesureValues.add(mesureAtDate);
            } catch (Exception e) {
                logger.severe(LOGGER_HEADER + " Can't decode PointOfInterest " + pointMap.toString() + " : " + e.getMessage());
            }
        }
    }
    /* ******************************************************************************** */
    /*                                                                                  */
    /*
     * Job parameters
     * /*
     */
    /* ******************************************************************************** */

    /**
     * get the value of parameters for this tour (definition is accessible via
     * plugIn.getDescription().inputParameters
     * 
     * @return
     */
    public Map<String, Object> getJobParameters() {
        return parameters;
    }

    /**
     * @param parameters
     */
    public void setJobParameters(Map<String, Object> parameters) {
        this.parameters = parameters == null ? new HashMap<>() : parameters;
    }


 
    /**
     * set a Stream parameters
     * 
     * @param parameterName
     * @param temporaryFileName
     * @param pageDirectory
     */
    public List<BEvent> setJobFileParameter(String parameterName, String temporaryFileName, File pageDirectory) {
        List<BEvent> listEvents = new ArrayList<>();
        List<String> listParentTmpFile = new ArrayList<>();
        try {
            listParentTmpFile.add(pageDirectory.getCanonicalPath() + "/../../../tmp/");
            listParentTmpFile.add(pageDirectory.getCanonicalPath() + "/../../");
        } catch (Exception e) {
            logger.severe(".setTourFileParameter: error get CanonicalPath of pageDirectory[" + e.toString() + "]");
            listEvents.add(eventCantFindTemporaryPath);
            return listEvents;
        }
        StringBuilder detectedPaths = new StringBuilder();
        boolean findFile = false;
        for (String pathTemp : listParentTmpFile) {
            // logger.fine(logHeader+".setTourFileParameter: CompleteuploadFile  TEST [" + pathTemp + temporaryFileName + "]");
            detectedPaths.append("[" + pathTemp + temporaryFileName + "],");
            if (new File(pathTemp + temporaryFileName).exists()) {
                try {
                    findFile = true;
                    FileInputStream fileinputStream = new FileInputStream(pathTemp + temporaryFileName);
                    // search the parameters
                    MilkPlugInDescription plugInDescription = getPlugIn().getDescription();

                    for (PlugInParameter parameter : plugInDescription.getInputParameters()) {
                        if (parameter.name.equals(parameterName) &&
                                (parameter.typeParameter == TypeParameter.FILEREAD || parameter.typeParameter == TypeParameter.FILEREADWRITE)) {
                            setParameterStream(parameter, fileinputStream);
                        }
                    }
                } catch (Exception e) {
                    logger.severeException(e, ".setTourFileParameter: File[" + pathTemp + temporaryFileName + "] ");
                    listEvents.add(new BEvent(eventCantFindTemporaryFile, e, "Path:" + pathTemp + "]File[" + temporaryFileName + "] Complete file[" + pathTemp + temporaryFileName + "]"));

                }
            }
        } // end look on pathDirectory
        if (!findFile)
            listEvents.add(new BEvent(eventCantFindTemporaryFile, "Path:" + pageDirectory.toString() + "]File[" + temporaryFileName + "] Detected path " + detectedPaths));
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

        // CSVOperation.saveFile("c:/temp/SetFile"+plugInParameter.name+".csv", stream);
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
        List<BEvent> listEvents = new ArrayList<>();
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

            // For debug
            // CSVOperation.saveFile("c:/temp/upload-"+plugInParameter.name+".csv", instream);
        } catch (IOException e) {
            listEvents.add(new BEvent(eventErrorReadingParamFile, e, "ParameterFile[" + plugInParameter.name + "]"));
            logger.severeException(e, ".getParameterStream:Error writing parameter ");
        }
        return listEvents;
    }

    /*
     * ******************************************************************************** *
     * Notification
     * ********************************************************************************
     */
    
    /**
     * A change is made in parameters (cron, parametres...) : notify the job
     * @param milkJobExecution
     */
    public List<BEvent> notifyAChangeInJob( MilkJobExecution milkJobExecution ) {
        return getPlugIn().notifyUpdateParameters(this, milkJobExecution);
    }
    
    
    /*
     * ******************************************************************************** *
     * Serialization (JSON)
     * We need to saved different properties, to ensure to not saved everything when needed.
     * Jobs has different component
     * - prefixPropertiesBase ==> getMap()
     * - prefixPropertiesAskStop => direct askStop()
     * - prefixPropertiesSavedExecution ==>
     * - prefixPropertiesTrackExecution ==>
     * ********************************************************************************
     */

    public final static String CSTJSON_CRON = "cron";
    public final static String CSTJSON_NB_SAVEDEXECUTION = "nbSavedExecution";

    public final static String CSTJSON_HAS_MESURES = "hasMesure";
    public final static String CSTJSON_NB_HISTORYMESURES = "nbHistoryMesures";
    public final static String CSTJSON_JOBCANBESTOPPED_ITEMS = "jobCanBeStoppedItems";
    public final static String CSTJSON_JOBCANBESTOPPED_DELAY = "jobCanBeStoppedDelay";
    public final static String CSTJSON_STOPAFTER_NBITEMS = "stopafterNbItems";
    public final static int CSTDEFAULT_STOPAFTER_NBITEMS = -1;
    public final static String CSTJSON_STOPAFTER_NBMINUTES = "stopafterNbMinutes";
    public final static int CSTDEFAULT_STOPAFTER_NBMINUTES = -1;

    public final static String CSTJSON_HOSTSRESTRICTION = "hostsrestriction";

    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * describe the plug tour
     * 
     * @return
     */

    private final static String CSTJSON_PREFIX_HUMANREADABLE = "st";
    private final static String CSTJSON_PLUGINNAME = "pluginname";
    private final static String CSTJSON_ENABLE = "enable";

    private final static String CSTJSON_PARAMETERS_DEF = "parametersdef";
    private final static String CSTJSON_ANALYSISDEF = "analysisdef";

    private final static String CSTJSON_PARAMETERS = "parameters";
    // private final static String cstJsonParameterValue = "value";

    public final static String CSTJSON_PARAMETER_DELAY_SCOPE = "scope";
    public final static String CSTJSON_PARAMETER_DELAY_VALUE = "value";

    private final static String CSTJSON_PLUGIN_DISPLAYNAME = "plugindisplayname";
    private final static String CSTJSON_DESCRIPTION = "description";
    private final static String CSTJSON_EXPLANATION = "explanation";
    private final static String CSTJSON_NEXTEXECUTION = "nextexecution";
    private final static String cstJsonLastExecution = "lastexecution";
    private final static String CSTJSON_NAME = "name";
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
    private final static String CSTJSON_SAVEDEXEC = "savedExecution";
    private final static String CSTJSON_SAVEEXEC_OVERLOAD ="savedExecutionOverload";
    
    private final static String cstJsonSaveExecDate = "execDate";
    private final static String cstJsonSaveExecStatus = "status";
    private final static String cstJsonSaveExecListEventsSt = "listevents";
    private final static String cstJsonSaveExecExplanation = "explanation";
    private final static String cstJsonSaveExecItemsProcessed = "itemprocessed";
    private final static String cstJsonSaveExecTimeinMs = "timeinms";
    private final static String cstJsonSaveExecHostName = "hostname";

    
    // Mesure
    
    public final static String CSTJSON_MESURE_PLUGIN_NAME = "name";
    public final static String CSTJSON_MESURE_PLUGIN_LABEL = "label";
    public final static String CSTJSON_MESURE_PLUGIN_EXPLANATION= "explanation";
    
    public final static String CSTJSON_MESURES ="mesures";
    public final static String CSTJSON_MESURE_DEF = "def";
    public final static String CSTJSON_MESURE_HISTORY ="history";
    
    public final static String CSTJSON_MESUREATDATE_DATEST="datest";
    public final static String CSTJSON_MESUREATDATE_VALUEMAP="valuemap";
    public final static String CSTJSON_MESUREATDATE_VALUELIST="valuelist";
    /**
     * getInstanceFromMap (the load)
     * 
     * @see getMap()
     * @param jsonSt
     * @param milkCmdControl
     * @return
     */

    @SuppressWarnings("unchecked")
    public static MilkJob getInstanceFromMap(Map<String, Object> jsonMap, MilkJobFactory milkJobFactory) {
        if (jsonMap == null)
            return null;

        String plugInName = (String) jsonMap.get(CSTJSON_PLUGINNAME);
        MilkPlugInFactory milkPlugInFactory = milkJobFactory.getMilkPlugInFactory();

        MilkPlugIn plugIn = milkPlugInFactory.getPluginFromName(plugInName);
        if (plugIn == null)
            return null;

        String name = (String) jsonMap.get(CSTJSON_NAME);
        MilkJob milkJob = new MilkJob(name, plugIn, milkJobFactory);
        milkJob.description = (String) jsonMap.get(CSTJSON_DESCRIPTION);

        milkJob.idJob = getLongValue(jsonMap.get(cstJsonId), 0L);

        // clone the parameters !
        // new HashMap<>(description.getParametersMap()) not need at this moment because the maps is created
        milkJob.parameters = (Map<String, Object>) jsonMap.get(CSTJSON_PARAMETERS);

        @SuppressWarnings("unused")
        List<Map<String, Object>> listParametersDef = (List<Map<String, Object>>) jsonMap.get(CSTJSON_PARAMETERS_DEF);

        milkJob.cronSt = (String) jsonMap.get(CSTJSON_CRON);
        milkJob.nbSavedExecution = TypesCast.getInteger(jsonMap.get(CSTJSON_NB_SAVEDEXECUTION), milkJob.plugIn.getDefaultNbSavedExecution());
        milkJob.nbHistoryMesure = TypesCast.getInteger(jsonMap.get(CSTJSON_NB_HISTORYMESURES), plugIn.getDefaultNbHistoryMesures());

        milkJob.stopAfterNbItems = TypesCast.getInteger(jsonMap.get(CSTJSON_STOPAFTER_NBITEMS), CSTDEFAULT_STOPAFTER_NBITEMS);
        milkJob.stopAfterNbMinutes = TypesCast.getInteger(jsonMap.get(CSTJSON_STOPAFTER_NBMINUTES), CSTDEFAULT_STOPAFTER_NBMINUTES);

        milkJob.hostsRestriction = (String) jsonMap.get(CSTJSON_HOSTSRESTRICTION);

        // search the name if all the list
        milkJob.isEnable = getBooleanValue(jsonMap.get(CSTJSON_ENABLE), false);

        //  milkJob.trackExecution.readTrackExecutionFromMap(jsonMap);

        /*
         * milkJob.trackExecution.askForStop = getBooleanValue(jsonMap.get(cstJsonAskForStop), false);
         * Long lastExecutionDateLong = (Long) jsonMap.get(cstJsonLastExecution);
         * if (lastExecutionDateLong != null && lastExecutionDateLong != 0)
         * milkJob.trackExecution.lastExecutionDate = new Date(lastExecutionDateLong);
         * String lastExecutionStatus = (String) jsonMap.get(cstJsonlastExecutionStatus);
         * if (lastExecutionStatus != null)
         * milkJob.trackExecution.lastExecutionStatus = ExecutionStatus.valueOf(lastExecutionStatus.toUpperCase());
         * milkJob.trackExecution.isImmediateExecution = getBooleanValue(jsonMap.get(cstJsonImmediateExecution), false);
         * milkJob.trackExecution.inExecution = getBooleanValue(jsonMap.get(cstJsonInExecution), false);
         * milkJob.trackExecution.startTime = getLongValue(jsonMap.get(cstJsonInExecutionStartTime), 0L);
         * milkJob.trackExecution.percent = getLongValue(jsonMap.get(cstJsonInExecutionPercent), 0L);
         * milkJob.trackExecution.endTimeEstimatedInMs = getLongValue(jsonMap.get(cstJsonInExecutionEndTimeEstimatedInMS), 0L);
         * milkJob.trackExecution.endDateEstimated = null;
         * if (milkJob.trackExecution.startTime > 0 && milkJob.trackExecution.endTimeEstimatedInMs > 0) {
         * milkJob.trackExecution.endDateEstimated = new Date(milkJob.trackExecution.startTime + milkJob.trackExecution.endTimeEstimatedInMs);
         * }
         * milkJob.trackExecution.inExecutionHostName = (String) jsonMap.get(cstJsonRegisterInExecutionHost);
         */

        /*
         * Do not calculate this : we read the job section per section
         * if (milkJob.isEnable && milkJob.trackExecution.nextExecutionDate == null)
         * milkJob.calculateNextExecution("GetInstanceFromJson, Enable and not date");
         */

        // get the last saved execution
        // nota: MilkSerialize saved this information in a different variable, then it overide this value by calling the getreadSavedExecution() methiod
        /*
         * List<Map<String, Object>> list = (List<Map<String, Object>>) jsonMap.get(cstJsonSavedExec);
         * if (list != null) {
         * for (Map<String, Object> execSaveMap : list) {
         * milkJob.listSavedExecution.add(SavedExecution.getInstance(execSaveMap));
         * }
         * }
         */
        return milkJob;
    }

    public void readSavedExecutionFromList(List<Map<String, Object>> jsonList) {
        if (jsonList != null) {
            for (Map<String, Object> execSaveMap : jsonList) {
                listSavedExecution.add(SavedExecution.getInstance(execSaveMap));
            }
        }
    }

    public void readTrackExecutionFromMap(Map<String, Object> jsonMap) {
        trackExecution.readTrackExecutionFromMap(jsonMap);
    }

    /**
     * Getter for Serialisation
     */
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
        public boolean limitData = false;
        public boolean mesures = true;
        public boolean destinationHtml=false;

        public static MapContentParameter getInstanceWeb() {
            MapContentParameter mapContentParameter = new MapContentParameter();
            mapContentParameter.withExplanation = true;
            mapContentParameter.withExplanation = true;
            mapContentParameter.askStop = true;
            mapContentParameter.savedExecution = true;
            mapContentParameter.destinationHtml = true;
            return mapContentParameter;
        }
    }

    /**
     * getMap : use to save it or send to the browser.
     * browser need to have the parameters definition AND the value, save need only the parameter
     * value.
     * 
     * @return
     */
    public Map<String, Object> getMap(MapContentParameter mapContent) {

        Map<String, Object> map = new HashMap<>();
        map.put(CSTJSON_NAME, getName());
        map.put(cstJsonId, getId());
        map.put(CSTJSON_PLUGINNAME, plugIn.getName());
        map.put(CSTJSON_DESCRIPTION, description);
        if (mapContent.withExplanation) {
            map.put(CSTJSON_EXPLANATION, plugIn.getDescription().getExplanation());
            map.put(CSTJSON_PLUGIN_DISPLAYNAME, plugIn.getDescription().getLabel());
        }
        map.put(CSTJSON_CRON, cronSt);
        map.put(CSTJSON_NB_SAVEDEXECUTION, nbSavedExecution);
        map.put(CSTJSON_HAS_MESURES, plugIn.getDescription().hasMesures());
        map.put(CSTJSON_NB_HISTORYMESURES, nbHistoryMesure);
        map.put(CSTJSON_JOBCANBESTOPPED_ITEMS, plugIn.getDescription().isJobCanBeStopByItemsMaximum());
        map.put(CSTJSON_JOBCANBESTOPPED_DELAY, plugIn.getDescription().isJobCanBeStopByDelay());
        map.put(CSTJSON_STOPAFTER_NBITEMS, stopAfterNbItems);
        map.put(CSTJSON_STOPAFTER_NBMINUTES, stopAfterNbMinutes);

        map.put(CSTJSON_HOSTSRESTRICTION, hostsRestriction);

        // Parameters values
        Map<String, Object> mapParametersValue = new HashMap<>();
        map.put(CSTJSON_PARAMETERS, mapParametersValue);

        // parameter definition
        map.put(CSTJSON_PARAMETERS_DEF, collectParameterList(plugIn.getDefinitionDescription().getInputParameters(), mapParametersValue));

        // Analysis definition
        map.put(CSTJSON_ANALYSISDEF, collectParameterList(plugIn.getDefinitionDescription().getAnalysisParameters(), mapParametersValue));

        map.put(CSTJSON_ENABLE, isEnable);

        if (mapContent.trackExecution) {
            map.put("trackExecution", trackExecution.getMap());

        }
        if (mapContent.askStop) {
            map.put(cstJsonAskForStop, trackExecution.askForStop);
        }
        if (mapContent.savedExecution) {
            map.put(CSTJSON_SAVEDEXEC, getMapListSavedExecution(mapContent.limitData));
        }
        map.put( CSTJSON_SAVEEXEC_OVERLOAD, Boolean.valueOf(listSavedExecution.size() > CSTMAXLIMITEDDATA));
        
        if (mapContent.mesures) {
            map.put(CSTJSON_MESURES, getMapMesures(mapContent.limitData, mapContent.destinationHtml));
        }
        return map;
    }

    public Map<String, Object> getTrackExecution() {
        return trackExecution.getMap();
    }

    public List<Map<String, Object>> getMapListSavedExecution(boolean limitData) {
        // save the last execution
        List<Map<String, Object>> listExecution = new ArrayList<>();
        for (SavedExecution savedExecution : listSavedExecution) {
            if (limitData && listExecution.size() > CSTMAXLIMITEDDATA)
                break;
            listExecution.add(savedExecution.getMap());
        }
        return listExecution;
    }

    /**
     * Produce 2 formats:
     *   -"def" : [ { name : Mesure 1, label : Mesure 1 MLabel, "explanation" }
     *          Mesure Name 1 / Mesure Name 2
     *   - "history"
     *       [ {"date": Date , "values" :  [Value 1, value 2 ]}]
     * @param limitData
     * @return
     */
    public Map<String, Object> getMapMesures( boolean limitData, boolean destinationHtml ) {
        
        Map<String, Object> listMesures = new HashMap<>();
        List<Map<String,Object>> listDef = new ArrayList<>();
        for (PlugInMesure plugInMesure : plugIn.getDescription().getMesures())
        {
            listDef.add( plugInMesure.getMap());
        }
        listMesures.put( CSTJSON_MESURE_DEF, listDef);
        // now, values
       listMesures.put( CSTJSON_MESURE_HISTORY, getListMesureValues( destinationHtml));
        
        return listMesures;
    
    }
    
    /*
     * ******************************************************************************** *
     * Parameters (JSON)
     * ********************************************************************************
     */

    private List<Map<String, Object>> collectParameterList(List<PlugInParameter> listPlugInParameters, Map<String, Object> mapParametersValue) {
        List<Map<String, Object>> list = new ArrayList<>();
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

            // Definition may change: this is the time to check the content
            boolean valueIsAnArray = false;
            if (plugInParameter.typeParameter == TypeParameter.ARRAY || plugInParameter.typeParameter == TypeParameter.ARRAYMAP || plugInParameter.typeParameter == TypeParameter.ARRAYPROCESSNAME)
                valueIsAnArray = true;

            if (valueIsAnArray && mapParametersValue.get(plugInParameter.name) != null && (!(mapParametersValue.get(plugInParameter.name) instanceof List))) {
                List<Object> listObject = new ArrayList<>();
                listObject.add(mapParametersValue.get(plugInParameter.name));
                mapParametersValue.put(plugInParameter.name, listObject);
            }
            if (plugInParameter.typeParameter == TypeParameter.DELAY) {
                if (!(mapParametersValue.get(plugInParameter.name) instanceof Map)) {
                    Map<String, Object> valueMapDelay = new HashMap<>();
                    Object valueScope = parameters.get(plugInParameter.name);
                    if (valueScope == null)
                        valueScope = plugInParameter.defaultValue;
                    if (valueScope == null)
                        valueScope = "";
                    DelayResult delayResult = MilkPlugInToolbox.getTimeFromDelay(null, valueScope.toString(), new Date(), true);
                    valueMapDelay.put(CSTJSON_PARAMETER_DELAY_SCOPE, delayResult.scopeInput.toString());
                    valueMapDelay.put(CSTJSON_PARAMETER_DELAY_VALUE, delayResult.delayInput);
                    mapParametersValue.put(plugInParameter.name, valueMapDelay);
                }
            }
            // Parameters Definition
            list.add(plugInParameter.getMap());
        }
        return list;
    }

    private void generateId() {
        // sleep a little to be sure to have a unique ID in case of a loop
        try {
            Thread.sleep(2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
    private class TrackExecution {

        /**
         * Job will be start at the next heartBeat.
         * See trackExecution attribute to describe an executiong job
         */
        private boolean isImmediateExecution = false;

        public boolean askForStop = false;
        /**
         * keep the last Execution Date and Status, for the dashboard
         */
        private Date nextExecutionDate;
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
        };

        public Map<String, Object> getMap() {
            Map<String, Object> map = new HashMap<>();

            map.put(cstJsonImmediateExecution, trackExecution.isImmediateExecution);
            map.put(cstJsonAskForStop, trackExecution.askForStop);
            map.put(CSTJSON_NEXTEXECUTION, trackExecution.nextExecutionDate == null ? 0 : trackExecution.nextExecutionDate.getTime());
            map.put(CSTJSON_NEXTEXECUTION + CSTJSON_PREFIX_HUMANREADABLE, trackExecution.nextExecutionDate == null ? "" : sdf.format(trackExecution.nextExecutionDate.getTime()));
            map.put(cstJsonLastExecution, trackExecution.lastExecutionDate == null ? 0 : trackExecution.lastExecutionDate.getTime());
            map.put(cstJsonLastExecution + CSTJSON_PREFIX_HUMANREADABLE, trackExecution.lastExecutionDate == null ? "" : sdf.format(trackExecution.lastExecutionDate));
            String executionStatus = trackExecution.lastExecutionStatus == null ? null : trackExecution.lastExecutionStatus.toString().toLowerCase();
            map.put(cstJsonlastExecutionStatus, executionStatus);

            map.put(cstJsonInExecution, trackExecution.inExecution);
            map.put(cstJsonInExecutionStartTime, trackExecution.startTime);
            map.put(cstJsonInExecutionPercent, trackExecution.percent);
            map.put(cstJsonInExecutionEndTimeEstimatedInMS, trackExecution.endTimeEstimatedInMs);
            map.put(cstJsonInExecutionEndTimeEstimatedInMS + CSTJSON_PREFIX_HUMANREADABLE, getHumanTimeEstimated(false));
            map.put(cstJsonInExecutionEndDateEstimated + CSTJSON_PREFIX_HUMANREADABLE, trackExecution.endDateEstimated == null ? "" : sdf.format(trackExecution.endDateEstimated));
            map.put(cstJsonRegisterInExecutionHost, trackExecution.inExecutionHostName);
            return map;
        }

        public void readTrackExecutionFromMap(Map<String, Object> jsonMap) {
            //  Map<String, Object> jsonMap = (Map<String, Object>) JSONValue.parse(jsonSt);
            isImmediateExecution = getBooleanValue(jsonMap.get(cstJsonImmediateExecution), false);
            askForStop = getBooleanValue(jsonMap.get(cstJsonAskForStop), false);

            Long nextExecutionDateLong = (Long) jsonMap.get(CSTJSON_NEXTEXECUTION);
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

        public SavedExecution(MilkJobOutput output) {
            executionDate = output.executionDate;
            executionStatus = output.executionStatus;
            listEventSt = BEventFactory.getSyntheticHtml(output.getListEvents());
            explanation = output.explanation;
            nbItemsProcessed = output.nbItemsProcessed;
            executionTimeInMs = output.executionTimeInMs;
            hostName = output.hostName;
        }

        public Map<String, Object> getMap() {
            Map<String, Object> map = new HashMap<>();
            map.put(cstJsonSaveExecDate, executionDate.getTime());
            map.put(cstJsonSaveExecDate + CSTJSON_PREFIX_HUMANREADABLE, sdf.format(executionDate));
            map.put(cstJsonSaveExecStatus, executionStatus.toString());
            map.put(cstJsonSaveExecListEventsSt, listEventSt);
            map.put(cstJsonSaveExecExplanation, explanation);
            map.put(cstJsonSaveExecItemsProcessed, nbItemsProcessed);
            map.put(cstJsonSaveExecTimeinMs, executionTimeInMs);
            map.put(cstJsonSaveExecTimeinMs + CSTJSON_PREFIX_HUMANREADABLE, TypesCast.getHumanDuration(executionTimeInMs, false));

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

    // save a job execution
    // keep a history of the last X execution. X is a parameters for each job.
    List<SavedExecution> listSavedExecution = new ArrayList<>();

    /**
     * register an execution. Keep the last 10
     * 
     * @param currentDate
     * @param output
     */
    public void registerExecution(Date currentDate, MilkJobOutput output) {
        trackExecution.lastExecutionDate = output.executionDate;
        trackExecution.lastExecutionStatus = output.executionStatus;
        if (output.executionStatus == ExecutionStatus.SUCCESSNOTHING
                || output.executionStatus == ExecutionStatus.NOEXECUTION) {
            return; // no need to save it
        }

        SavedExecution savedExecution = new SavedExecution(output);
        listSavedExecution.add(0, savedExecution);

        if (listSavedExecution.size() > nbSavedExecution)
            listSavedExecution = listSavedExecution.subList(0, nbSavedExecution - 1);
    }
}
