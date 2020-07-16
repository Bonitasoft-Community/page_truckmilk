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
import org.bonitasoft.truckmilk.engine.MilkConstantJson;
import org.bonitasoft.truckmilk.engine.MilkJobFactory;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInMeasurement;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.ReplacementPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInFactory;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.DelayResult;
import org.bonitasoft.truckmilk.toolbox.MilkLog;
import org.bonitasoft.truckmilk.toolbox.TypesCast;
import org.json.simple.JSONValue;
import org.quartz.CronExpression;

import lombok.Data;

/* ******************************************************************************** */
/*                                                                                  */
/* MilkJob */
/*                                                                                  */
/*
 * The milkJob is a container, to describe information on the job.
 * Execution under the responsability of the MilkPlugIn class
 * Execution information is manage via the MilkJobExecution container
 * A new plugin instance is requested, to be start every 5 mn for example.
 * A plugInTour reference a PlugIn (embeded or not), a Schedule (frequency and next start) and
 * parameters
 */
/*                                                                                  */
/* ******************************************************************************** */

public @Data class MilkJob {

    private final static MilkLog logger = MilkLog.getLogger(MilkJob.class.getName());
    private final static String LOGGER_HEADER = "MilkJob ";
    
    private final SimpleDateFormat sdfSynthetic = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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

    public enum SAVEDEXECUTIONPOLICY {
        SAVEALL, SAVEONLYWORK
    }

    public SAVEDEXECUTIONPOLICY savedExecutionPolicy = SAVEDEXECUTIONPOLICY.SAVEALL;
    /*
     * When the execution did something wrong like a BadConfiguration, then save the next one, event if it was SUCCESSNOTHING.
     */
    public String cronSt = "";

    public int nbSavedExecution;
    public int nbHistoryMesure;

    private int userStopAfterMaxItems;
    private int userStopAfterMaxMinutes;

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
        BADCONFIGURATION,
        /**
         * Not a final execution status, just report that the job start
         */
        START
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
        return name + "(" + idJob + "): " + (isEnable ? "ENABLE " : "disable ")
                + (trackExecution.isImmediateExecution ? "[ IMMEDIAT ] " : "")
                + (trackExecution.inExecution ? "[ EXECUTION ] " : "")
                + (trackExecution.nextExecutionDate == null ? "NoNextDate" : sdf.format(trackExecution.nextExecutionDate));
    }

    public String getHtmlInfo() {
        return "<b>" + name + "</b> (" + idJob + ") " + (isEnable ? "<div style=\"color:green\">ENABLE</div>" : "disable ")
                + (trackExecution.isImmediateExecution ? "<div style=\"color:blue\">&ltIMMEDIAT&gt</div>" : "")
                + (trackExecution.inExecution ? "<div style=\"color:blue\">&ltEXECUTION&gt</div>" : "")
                + (trackExecution.nextExecutionDate == null ? "NoNextDate" : sdf.format(trackExecution.nextExecutionDate)) + ")";
    }

    public long getId() {
        return idJob;
    }

    public String getName() {
        return name == null ? MilkJob.DEFAULT_NAME : name;
    }

    public void setName(String name) {
        if (name == null)
            name = MilkJob.DEFAULT_NAME;
        this.name = name;
    }

    public MilkPlugIn getPlugIn() {
        return plugIn;
    }

    /**
     * this is the definition value.
     * Plug In should use the MilkJobExecution.getJobStopAfterMaxItems(), which is the combinaison of the value given by the user, the default value
     * (plugIn.getJobStopMaxItems() and any updated value
     * 
     * @return
     */
    public int getStopAfterMaxItems() {
        return userStopAfterMaxItems;
    }

    /**
     * this is the definition value.
     * Plug In should use the MilkJobExecution.getJobStopAfterMaxMinutes(), which is the combinaison of the value given by the user, the default value
     * (plugIn.getJobStopMaxItems() and any updated value
     * 
     * @return
     */
    public int getStopAfterMaxMinutes() {
        return userStopAfterMaxMinutes;
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


    public long getPercent() {
        return this.trackExecution.percent;
    }

    public void setPercent(long percent) {
        this.trackExecution.percent = percent;
    }

    public void setAvancementInformation(String avancementInformation) {
        this.trackExecution.avancementInformation = avancementInformation;
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

    private long lastTimeWeAskedForAStop = 0;

    /**
     * reload information for the database: user can ask a stop
     * 
     * @return
     */
    public boolean isAskedForStop() {
        // we update the status from the database every 60 s, else maximum of the CPU is take into a load the properties
        if (System.currentTimeMillis() - lastTimeWeAskedForAStop < 1000 * 60)
            return trackExecution.askForStop;
        lastTimeWeAskedForAStop = System.currentTimeMillis();

        trackExecution.askForStop = milkJobFactory.askStop(idJob);
        return trackExecution.askForStop;
    }

    public void setAskForStop(boolean askForStop) {
        this.trackExecution.askForStop = askForStop;
    }

    public boolean getAskForStop() {
        return this.trackExecution.askForStop;
    }

    public boolean inExecution() {
        return trackExecution.inExecution;
    }

    public void setInExecution(boolean inExecution) {
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
     * Measure
     * /*
     */
    /* ******************************************************************************** */
    public static class MeasurementAtDate {

        public Date date;
        public Map<PlugInMeasurement, Double> values;

        public MeasurementAtDate(Date date) {
            this.date = date;
        }
    }

    private List<MeasurementAtDate> listAllMeasurementValues = new ArrayList<>();

    public void addMesureValues(Date date, Map<PlugInMeasurement, Double> values) {
        MeasurementAtDate mesure = new MeasurementAtDate(date);
        mesure.values = values;
        listAllMeasurementValues.add(0, mesure);
        if (listAllMeasurementValues.size() > getNbHistoryMesure()
                && listSavedExecution.size() > nbSavedExecution) {
            listAllMeasurementValues = listAllMeasurementValues.subList(0, getNbHistoryMesure() - 1);
        }

        // limit the size too
        int estimationMaxItemsAccordingTheSize = estimateMaxItems(listAllMeasurementValues);
        if (listAllMeasurementValues.size() > estimationMaxItemsAccordingTheSize) {
            listAllMeasurementValues = listAllMeasurementValues.subList(0, estimationMaxItemsAccordingTheSize);
        }
    }

    /**
     * for serialization
     * listOfValues is true : then the result is a list of value, following the plugInDescription (usefull for the display)
     * [ {"date": Date , "valueslist" : [12.4,43 ]}]
     * false : a map <MesureName, mesureValue> : better for the serialization, when a new mesure can be add between two saves.
     * [ {"date": Date , "values" : {"Temperature": 12.4", "Weight":43]
     * 
     * @return
     */
    public List<Map<String, Object>> getListMeasurementValues(boolean listOfValues) {
        List<Map<String, Object>> listPoints = new ArrayList<>();
        for (MeasurementAtDate mesureAtDate : listAllMeasurementValues) {
            Map<String, Object> mapMesureValue = new HashMap<>();
            mapMesureValue.put(MilkConstantJson.CSTJSON_MEASUREMENTATDATE_DATEST, mesureAtDate.date == null ? "" : sdf.format(mesureAtDate.date));
            if (listOfValues) {
                List<Double> listValues = new ArrayList<>();
                for (PlugInMeasurement plugInMesure : plugIn.getDescription().getListMesures()) {
                    listValues.add(mesureAtDate.values.get(plugInMesure));
                }
                mapMesureValue.put(MilkConstantJson.CSTJSON_MEASUREMENTATDATE_VALUELIST, listValues);
            } else {
                Map<String, Double> mapValues = new HashMap<>();
                for (Entry<PlugInMeasurement, Double> entry : mesureAtDate.values.entrySet()) {
                    mapValues.put(entry.getKey().getName(), entry.getValue());
                }
                mapMesureValue.put(MilkConstantJson.CSTJSON_MEASUREMENTATDATE_VALUEMAP, mapValues);

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
    public void readMeasurementValuesFromList(List<Map<String, Object>> listMesures) {
        listAllMeasurementValues = new ArrayList<>();
        for (Map<String, Object> pointMap : listMesures) {
            try {
                Date dateMesure = sdf.parse((String) pointMap.get(MilkConstantJson.CSTJSON_MEASUREMENTATDATE_DATEST));
                MeasurementAtDate mesureAtDate = new MeasurementAtDate(dateMesure);

                mesureAtDate.values = new HashMap<>();

                for (Entry<String, Double> entry : ((Map<String, Double>) pointMap.get(MilkConstantJson.CSTJSON_MEASUREMENTATDATE_VALUEMAP)).entrySet()) {
                    PlugInMeasurement mesure = getPlugIn().getDescription().getMeasureFromName(entry.getKey());
                    if (mesure != null)
                        mesureAtDate.values.put(mesure, entry.getValue());
                }
                listAllMeasurementValues.add(mesureAtDate);
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
                try (FileInputStream fileinputStream = new FileInputStream(pathTemp + temporaryFileName)) {
                    findFile = true;
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

            return ((compareHostRestriction.indexOf(";" + ip.getHostAddress() + ";") != -1) ||
                    (compareHostRestriction.indexOf(";" + ip.getHostName() + ";") != -1));
        } catch (UnknownHostException e) {
            return false;
        }
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /*
     * Parameters File
     * /*
     */
    /* ******************************************************************************** */
    public void setParameterValue(PlugInParameter plugInParameter, Object value) {
        parameters.put(plugInParameter.name, value);
    }

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
     * 
     * @param milkJobExecution
     */
    public List<BEvent> notifyAChangeInJob(MilkJobExecution milkJobExecution) {
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

    public final static int CSTDEFAULT_STOPAFTER_MAXITEMS = -1;
    public final static int CSTDEFAULT_STOPAFTER_MAXMINUTES = -1;

    //squid:S2885 propose to not use it as Static, but used in a static method
    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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

        String plugInName = (String) jsonMap.get(MilkConstantJson.CSTJSON_JOB_PLUGINNAME);
        MilkPlugInFactory milkPlugInFactory = milkJobFactory.getMilkPlugInFactory();

        String name = (String) jsonMap.get(MilkConstantJson.CSTJSON_JOB_NAME);
        MilkJob milkJob = null;
        Map<String, Object> parametersMap = (Map<String, Object>) jsonMap.get(MilkConstantJson.CSTJSON_PARAMETERS);

        MilkPlugIn plugIn = milkPlugInFactory.getPluginFromName(plugInName);
        if (plugIn == null) {
            // there is  maybe a replacement ?
            ReplacementPlugIn replacementPlugIn = milkPlugInFactory.getReplacement(plugInName, parametersMap);
            if (replacementPlugIn == null)
                return null;
            plugIn = replacementPlugIn.plugIn;
            parametersMap = replacementPlugIn.parameters;
        }

        milkJob = new MilkJob(name, plugIn, milkJobFactory);

        milkJob.parameters = parametersMap;

        milkJob.description = (String) jsonMap.get(MilkConstantJson.CSTJSON_JOB_DESCRIPTION);

        milkJob.idJob = getLongValue(jsonMap.get(MilkConstantJson.CSTJSON_JOB_ID), 0L);

        
        // List<Map<String, Object>> listParametersDef = (List<Map<String, Object>>) jsonMap.get(MilkConstantJson.CSTJSON_PARAMETERS_DEF);

        milkJob.cronSt = (String) jsonMap.get(MilkConstantJson.CSTJSON_JOB_CRON);
        try {
            milkJob.savedExecutionPolicy = SAVEDEXECUTIONPOLICY.valueOf((String) jsonMap.get(MilkConstantJson.CSTJSON_SAVEDEXECUTIONPOLICY));
        } catch (Exception e) {
            milkJob.savedExecutionPolicy = SAVEDEXECUTIONPOLICY.SAVEALL;
        }
        milkJob.nbSavedExecution = TypesCast.getInteger(jsonMap.get(MilkConstantJson.CSTJSON_NB_SAVEDEXECUTION), milkJob.plugIn.getDefaultNbSavedExecution());
        milkJob.nbHistoryMesure = TypesCast.getInteger(jsonMap.get(MilkConstantJson.CSTJSON_NB_HISTORYMEASUREMENT), plugIn.getDefaultNbHistoryMesures());

        milkJob.userStopAfterMaxItems = TypesCast.getInteger(jsonMap.get(MilkConstantJson.CSTJSON_STOPAFTER_NBITEMS), CSTDEFAULT_STOPAFTER_MAXITEMS);
        milkJob.userStopAfterMaxMinutes = TypesCast.getInteger(jsonMap.get(MilkConstantJson.CSTJSON_STOPAFTER_NBMINUTES), CSTDEFAULT_STOPAFTER_MAXMINUTES);

        milkJob.hostsRestriction = (String) jsonMap.get(MilkConstantJson.CSTJSON_HOSTSRESTRICTION);

        // search the name if all the list
        milkJob.isEnable = getBooleanValue(jsonMap.get(MilkConstantJson.CSTJSON_ENABLE), false);

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

        public boolean withExplanation = false;
        public boolean withHeader = true;
        public boolean withTrackExecution = true;
        public boolean withParameters = true;
        public boolean withParametersFile = false;
        public boolean withAskStop = true;
        public boolean withSavedExecutionHeader = true;
        public boolean withSavedExecutionDetails = true;
        public String filterSavedExecution = null;
        public boolean limitData = false;
        public boolean withMeasures = true;
        public boolean destinationHtml = false;

        public long pageStart = 0;
        public long pageCount = 100;

        public static MapContentParameter getInstanceWeb() {
            MapContentParameter mapContentParameter = new MapContentParameter();
            mapContentParameter.withExplanation = true;
            mapContentParameter.withHeader = true;
            mapContentParameter.withParameters = false;
            mapContentParameter.withParametersFile = true;
            mapContentParameter.withAskStop = true;
            mapContentParameter.withSavedExecutionHeader = true;
            mapContentParameter.withSavedExecutionDetails = false;
            mapContentParameter.withMeasures = false;
            mapContentParameter.destinationHtml = true;
            mapContentParameter.pageStart = 0;
            mapContentParameter.pageCount = 10;
            return mapContentParameter;
        }

        public static MapContentParameter getInstanceWebMinimum() {
            MapContentParameter mapContentParameter = new MapContentParameter();
            mapContentParameter.withExplanation = false;
            mapContentParameter.withHeader = false;
            mapContentParameter.withAskStop = false;
            mapContentParameter.withParameters = false;
            mapContentParameter.withParametersFile = false;
            mapContentParameter.withSavedExecutionHeader = false;
            mapContentParameter.withSavedExecutionDetails = false;
            mapContentParameter.withMeasures = false;
            mapContentParameter.limitData = false;

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
    public Map<String, Object> getMap(MapContentParameter mapContent, MilkJobContext milkJobContext) {

        Map<String, Object> map = new HashMap<>();
        map.put(MilkConstantJson.CSTJSON_JOB_NAME, getName());
        map.put(MilkConstantJson.CSTJSON_JOB_ID, getId());
        map.put(MilkConstantJson.CSTJSON_JOB_PLUGINNAME, plugIn.getName());
        if (mapContent.withHeader) {
            map.put(MilkConstantJson.CSTJSON_JOB_DESCRIPTION, description);
            map.put(MilkConstantJson.CSTJSON_JOB_CRON, cronSt);
            map.put(MilkConstantJson.CSTJSON_HOSTSRESTRICTION, hostsRestriction);
            map.put(MilkConstantJson.CSTJSON_ENABLE, isEnable);
            map.put(MilkConstantJson.CSTJSON_SAVEDEXECUTIONPOLICY, savedExecutionPolicy == null ? SAVEDEXECUTIONPOLICY.SAVEALL : savedExecutionPolicy.toString());
            map.put(MilkConstantJson.CSTJSON_NB_SAVEDEXECUTION, nbSavedExecution);
            map.put(MilkConstantJson.CSTJSON_NB_HISTORYMEASUREMENT, nbHistoryMesure);
            map.put(MilkConstantJson.CSTJSON_STOPAFTER_NBITEMS, userStopAfterMaxItems);
            map.put(MilkConstantJson.CSTJSON_STOPAFTER_NBMINUTES, userStopAfterMaxMinutes);

        }
        if (mapContent.withParameters) {
            // Parameters values
            Map<String, Object> mapParametersValue = new HashMap<>();

            // parameter definition
            map.put(MilkConstantJson.CSTJSON_PARAMETERS_DEF, collectParameterList(plugIn.getDefinitionDescription(milkJobContext).getInputParameters(), mapParametersValue, mapContent));

            // Analysis definition
            map.put(MilkConstantJson.CSTJSON_ANALYSISDEF, collectParameterList(plugIn.getDefinitionDescription(milkJobContext).getAnalysisParameters(), mapParametersValue, mapContent));
            map.put(MilkConstantJson.CSTJSON_PARAMETERS, mapParametersValue);
        } else if (mapContent.withParametersFile) {
            Map<String, Object> mapParametersValue = new HashMap<>();
            // only the parameters file
            List<PlugInParameter> listPlugInParametersFile = new ArrayList<>();
            for (PlugInParameter parameter : plugIn.getDefinitionDescription(milkJobContext).getInputParameters()) {
                if (TypeParameter.FILEREAD.equals(parameter.typeParameter)
                        || TypeParameter.FILEREADWRITE.equals(parameter.typeParameter)
                        || TypeParameter.FILEWRITE.equals(parameter.typeParameter))
                    listPlugInParametersFile.add(parameter);
            }
            // parameter definition
            map.put(MilkConstantJson.CSTJSON_PARAMETERS_DEF, collectParameterList(listPlugInParametersFile, mapParametersValue, mapContent));
            map.put(MilkConstantJson.CSTJSON_PARAMETERS, mapParametersValue);
        }
        if (mapContent.withExplanation) {
            map.put(MilkConstantJson.CSTJSON_JOB_PLUGIN_EXPLANATION, plugIn.getDescription().getExplanation());
            map.put(MilkConstantJson.CSTJSON_JOB_PLUGINDISPLAYNAME, plugIn.getDescription().getLabel());
            map.put(MilkConstantJson.CSTJSON_JOB_PLUGIN_WARNING, plugIn.getDescription().getWarning());
            map.put(MilkConstantJson.CSTJSON_HAS_MEASUREMENT, plugIn.getDescription().hasMesures());
            map.put(MilkConstantJson.CSTJSON_JOBCANBESTOPPED_ITEMS, plugIn.getDescription().isJobCanBeStopByMaxItems());
            map.put(MilkConstantJson.CSTJSON_JOBCANBESTOPPED_DELAY, plugIn.getDescription().isJobCanBeStopByMaxMinutes());
        }

        if (mapContent.withTrackExecution) {
            map.put(MilkConstantJson.CSTJSON_JOB_TRACKEXECUTION, trackExecution.getMap());

        }
        if (mapContent.withAskStop) {
            map.put(MilkConstantJson.CSTJSON_JOB_ASKFORSTOP, trackExecution.askForStop);
        }
        if (mapContent.withSavedExecutionHeader || mapContent.withSavedExecutionDetails) {
            List<Map<String, Object>> listSavedExecutionJob;
            if (mapContent.filterSavedExecution != null)
                listSavedExecutionJob = getMapListSavedExecutionFiltered(mapContent.filterSavedExecution, mapContent.destinationHtml);
            else
                listSavedExecutionJob = getMapListSavedExecution(mapContent.withSavedExecutionHeader, mapContent.withSavedExecutionDetails, mapContent.limitData, mapContent.pageStart, mapContent.pageCount, mapContent.destinationHtml);
            map.put(MilkConstantJson.CSTJSON_JOB_SAVEDEXEC, listSavedExecutionJob);
            map.put(MilkConstantJson.CSTJSON_JOB_SAVEEXEC_OVERLOAD, Boolean.valueOf(listSavedExecution.size() > CSTMAXLIMITEDDATA));
            map.put(MilkConstantJson.CSTJSON_JOB_SAVEDEXEC_MOREDATA, listSavedExecution.size() > listSavedExecutionJob.size());
        }

        if (mapContent.withMeasures) {
            map.put(MilkConstantJson.CSTJSON_JOB_MEASUREMENT, getMapMeasurement(mapContent.limitData, mapContent.destinationHtml));
        }
        return map;
    }

    public Map<String, Object> getTrackExecutionMap() {
        return trackExecution == null ? new HashMap<>() : trackExecution.getMap();
    }

    /**
     * get the number of SavedExecution
     * 
     * @param limitData
     * @param pageStart if different than -1, page mechanism is available. Start at page X (first page is 0)
     * @param pageCount Number of items per page.
     * @param destinationHtml
     * @return
     */
    public List<Map<String, Object>> getMapListSavedExecution(boolean withHeader, boolean withReportInHtml, boolean limitData, long pageStart, long pageCount, boolean destinationHtml) {
        // save the last execution
        List<Map<String, Object>> listExecution = new ArrayList<>();
        int count = 0;
        for (SavedExecution savedExecution : listSavedExecution) {
            count++;
            if (limitData && listExecution.size() > CSTMAXLIMITEDDATA)
                break;
            // page mechanism
            if (pageCount != -1) {
                if (count < pageCount * pageStart)
                    continue;
                if (count > pageCount * pageStart + pageCount)
                    continue;
            }
            listExecution.add(savedExecution.getMap(withHeader, withReportInHtml, destinationHtml));
        }
        return listExecution;
    }

    public List<Map<String, Object>> getMapListSavedExecutionFiltered(String filterDateExecution, boolean destinationHtml) {
        List<Map<String, Object>> listExecution = new ArrayList<>();
        for (SavedExecution savedExecution : listSavedExecution) {
            if (savedExecution.getExecutionDateSt().equals(filterDateExecution))
                listExecution.add(savedExecution.getMap(true, true, destinationHtml));
        }
        return listExecution;
    }

    /**
     * Produce 2 formats:
     * -"def" : [ { name : Mesure 1, label : Mesure 1 MLabel, "explanation" }
     * Mesure Name 1 / Mesure Name 2
     * - "history"
     * [ {"date": Date , "values" : [Value 1, value 2 ]}]
     * 
     * @param limitData
     * @return
     */
    public Map<String, Object> getMapMeasurement(boolean limitData, boolean destinationHtml) {

        Map<String, Object> listMesures = new HashMap<>();
        List<Map<String, Object>> listDef = new ArrayList<>();
        for (PlugInMeasurement plugInMesure : plugIn.getDescription().getMeasures()) {
            listDef.add(plugInMesure.getMap());
        }
        listMesures.put(MilkConstantJson.CSTJSON_MEASUREMENT_DEF, listDef);
        // now, values
        listMesures.put(MilkConstantJson.CSTJSON_MEASUREMENT_HISTORY, getListMeasurementValues(destinationHtml));

        return listMesures;

    }

    /*
     * ******************************************************************************** *
     * Parameters (JSON)
     * ********************************************************************************
     */

    private List<Map<String, Object>> collectParameterList(List<PlugInParameter> listPlugInParameters, Map<String, Object> mapParametersValue, MapContentParameter mapContent) {
        List<Map<String, Object>> list = new ArrayList<>();
        for (PlugInParameter plugInParameter : listPlugInParameters) {

            // complete with the value
            if (plugInParameter.typeParameter == TypeParameter.FILEREAD
                    || plugInParameter.typeParameter == TypeParameter.FILEREADWRITE
                    || plugInParameter.typeParameter == TypeParameter.FILEWRITE) {
                // Attention, in the parameters returned, the FILEWRITE parameters values must be removed
                // Skip this information in fact, will be save in a different way
                mapParametersValue.put(plugInParameter.name, parameters.get(plugInParameter.name + cstPrefixStreamValue) == null ? null : "AVAILABLE");
            } else if (plugInParameter.typeParameter == TypeParameter.INFORMATION) {
                // no need to manage that
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
            if (plugInParameter.typeParameter == TypeParameter.DELAY
                    && (!(mapParametersValue.get(plugInParameter.name) instanceof Map))) {
                Map<String, Object> valueMapDelay = new HashMap<>();
                Object valueScope = parameters.get(plugInParameter.name);
                if (valueScope == null)
                    valueScope = plugInParameter.defaultValue;
                if (valueScope == null)
                    valueScope = "";
                DelayResult delayResult = MilkPlugInToolbox.getTimeFromDelay(null, valueScope.toString(), new Date(), true);
                valueMapDelay.put(MilkConstantJson.CSTJSON_PARAMETER_DELAY_SCOPE, delayResult.scopeInput.toString());
                valueMapDelay.put(MilkConstantJson.CSTJSON_PARAMETER_DELAY_VALUE, delayResult.delayInput);
                mapParametersValue.put(plugInParameter.name, valueMapDelay);
            }

            // Parameters Definition
            list.add(plugInParameter.getMap(mapContent));
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
    public class TrackExecution {

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

        public String avancementInformation;
        public String inExecutionHostName;

        public TrackExecution() {
            // nothing to do at this moment in the constructor
        }

        public Map<String, Object> getMap() {
            Map<String, Object> map = new HashMap<>();

            map.put(MilkConstantJson.CSTJSON_JOB_IMMEDIATEEXECUTION, trackExecution.isImmediateExecution);
            map.put(MilkConstantJson.CSTJSON_JOB_ASKFORSTOP, trackExecution.askForStop);
            map.put(MilkConstantJson.CSTJSON_JOB_NEXTEXECUTION, trackExecution.nextExecutionDate == null ? 0 : trackExecution.nextExecutionDate.getTime());
            map.put(MilkConstantJson.CSTJSON_JOB_NEXTEXECUTION + MilkConstantJson.CSTJSON_PREFIX_HUMANREADABLE, TypesCast.getHumanDate( trackExecution.nextExecutionDate ) );
            map.put(MilkConstantJson.CSTJSON_JOB_LASTEXECUTION, trackExecution.lastExecutionDate == null ? 0 : trackExecution.lastExecutionDate.getTime());
            map.put(MilkConstantJson.CSTJSON_JOB_LASTEXECUTION + MilkConstantJson.CSTJSON_PREFIX_HUMANREADABLE, TypesCast.getHumanDate( trackExecution.lastExecutionDate ));
            String executionStatus = trackExecution.lastExecutionStatus == null ? null : trackExecution.lastExecutionStatus.toString().toLowerCase();
            map.put(MilkConstantJson.CSTJSON_LAST_EXECUTION_STATUS, executionStatus);

            map.put(MilkConstantJson.CSTJSON_IN_EXECUTION, trackExecution.inExecution);
            map.put(MilkConstantJson.CSTJSON_INEXECUTION_STARTTIME, trackExecution.startTime);
            map.put(MilkConstantJson.CSTJSON_INEXECUTION_PERCENT, trackExecution.percent);
            map.put(MilkConstantJson.CSTJSON_INEXECUTION_AVANCEMENTINFORMATION, avancementInformation);
            map.put(MilkConstantJson.cstJsonInExecutionEndTimeEstimatedInMS, trackExecution.endTimeEstimatedInMs);
            map.put(MilkConstantJson.cstJsonInExecutionEndTimeEstimatedInMS + MilkConstantJson.CSTJSON_PREFIX_HUMANREADABLE, getHumanTimeEstimated(false));
            map.put(MilkConstantJson.cstJsonInExecutionEndDateEstimated + MilkConstantJson.CSTJSON_PREFIX_HUMANREADABLE, TypesCast.getHumanDate( trackExecution.endDateEstimated ));
            map.put(MilkConstantJson.CSTJSON_INEXECUTION_HOST, trackExecution.inExecutionHostName);
            
            return map;
        }

        public void readTrackExecutionFromMap(Map<String, Object> jsonMap) {
            //  Map<String, Object> jsonMap = (Map<String, Object>) JSONValue.parse(jsonSt);
            isImmediateExecution = getBooleanValue(jsonMap.get(MilkConstantJson.CSTJSON_JOB_IMMEDIATEEXECUTION), false);
            askForStop = getBooleanValue(jsonMap.get(MilkConstantJson.CSTJSON_JOB_ASKFORSTOP), false);

            Long nextExecutionDateLong = (Long) jsonMap.get(MilkConstantJson.CSTJSON_JOB_NEXTEXECUTION);
            if (nextExecutionDateLong != null && nextExecutionDateLong != 0)
                trackExecution.nextExecutionDate = new Date(nextExecutionDateLong);

            Long lastExecutionDateLong = (Long) jsonMap.get(MilkConstantJson.CSTJSON_JOB_LASTEXECUTION);
            if (lastExecutionDateLong != null && lastExecutionDateLong != 0)
                trackExecution.lastExecutionDate = new Date(lastExecutionDateLong);

            String lastExecutionStatusReaded = (String) jsonMap.get(MilkConstantJson.CSTJSON_LAST_EXECUTION_STATUS);
            if (lastExecutionStatusReaded != null)
                trackExecution.lastExecutionStatus = ExecutionStatus.valueOf(lastExecutionStatusReaded.toUpperCase());

            inExecution = getBooleanValue(jsonMap.get(MilkConstantJson.CSTJSON_IN_EXECUTION), false);
            startTime = getLongValue(jsonMap.get(MilkConstantJson.CSTJSON_INEXECUTION_STARTTIME), 0L);
            percent = getLongValue(jsonMap.get(MilkConstantJson.CSTJSON_INEXECUTION_PERCENT), 0L);
            avancementInformation = (String) jsonMap.get(MilkConstantJson.CSTJSON_INEXECUTION_AVANCEMENTINFORMATION);

            endTimeEstimatedInMs = getLongValue(jsonMap.get(MilkConstantJson.cstJsonInExecutionEndTimeEstimatedInMS), 0L);
            endDateEstimated = null;
            if (startTime > 0 && endTimeEstimatedInMs > 0) {
                endDateEstimated = new Date(startTime + endTimeEstimatedInMs);
            }
            inExecutionHostName = (String) jsonMap.get(MilkConstantJson.CSTJSON_INEXECUTION_HOST);
        }

    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Saved execution */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    public static class SavedExecution {

        public Date executionDate;
        public ExecutionStatus executionStatus;

        private String listEventSt;
        public String reportInHtml;

        public long nbItemsProcessed = 0;
        /** null if no execution */
        public Long executionTimeInMs;
        public String hostName;

        public SavedExecution() {
            // nothing special to do right now
        }

        public SavedExecution(MilkJobOutput output) {
            this.executionDate = output.executionDate;
            this.executionStatus = output.executionStatus;
            this.listEventSt = BEventFactory.getSyntheticHtml(output.getListEvents());
            this.nbItemsProcessed = output.nbItemsProcessed;
            this.executionTimeInMs = output.executionTimeInMs;
            this.hostName = output.hostName;
            this.reportInHtml = output.getReportInHtml();
        }

        public SavedExecution(Date dateExecution, ExecutionStatus execution, String hostName, String reportInHtml, List<BEvent> listEvents) {
            this.executionDate = dateExecution;
            this.executionStatus = execution;
            this.listEventSt = BEventFactory.getSyntheticHtml(listEvents);
            this.hostName = hostName;
            this.reportInHtml = reportInHtml;

        }

        /**
         * return the report execution. Details maybe very huge, so depends on the result we wants
         * 
         * @param withHeader
         * @param withDetails
         * @param destinationHtml
         * @return
         */
        public Map<String, Object> getMap(boolean withHeader, boolean withDetails, boolean destinationHtml) {
            Map<String, Object> map = new HashMap<>();
            if (withHeader) {
                map.put(MilkConstantJson.CSTJSON_JOBEXECUTION_DATE, executionDate.getTime());
                map.put(MilkConstantJson.CSTJSON_JOBEXECUTION_DATE + MilkConstantJson.CSTJSON_PREFIX_HUMANREADABLE, getExecutionDateSt());
                // calculated a Tag to identify the day. This number is sequential and uni
                map.put(MilkConstantJson.CSTJSON_JOBEXECUTION_TAGDAY, String.valueOf(executionDate.getTime() / 1000 / 60 / 60 / 24));

                map.put(MilkConstantJson.CSTJSON_JOBEXECUTION_STATUS, executionStatus.toString());
                map.put(MilkConstantJson.CSTJSON_JOBEXECUTION_ITEMSPROCESSED, nbItemsProcessed);
                map.put(MilkConstantJson.CSTJSON_JOBEXECUTION_TIMEINMS, executionTimeInMs);
                if (executionTimeInMs != null)
                    map.put(MilkConstantJson.CSTJSON_JOBEXECUTION_TIMEINMS + MilkConstantJson.CSTJSON_PREFIX_HUMANREADABLE, TypesCast.getHumanDuration(executionTimeInMs, false));

                map.put(MilkConstantJson.CSTJSON_JOBEXECUTION_HOSTNAME, hostName);
            }
            if (withDetails) {
                map.put(MilkConstantJson.CSTJSON_JOBEXECUTION_LISTEVENTS_ST, listEventSt);
                map.put(MilkConstantJson.CSTJSON_JOBEXECUTION_REPORTINHTML, reportInHtml);
            }
            return map;
        }

        public String getExecutionDateSt() {
            return sdf.format(executionDate);
        }

        /**
         * @param jsonMap
         * @return
         */
        public static SavedExecution getInstance(Map<String, Object> jsonMap) {
            SavedExecution savedExecution = new SavedExecution();

            Long execDateLong = (Long) jsonMap.get(MilkConstantJson.CSTJSON_JOBEXECUTION_DATE);
            if (execDateLong != null && execDateLong != 0)
                savedExecution.executionDate = new Date(execDateLong);
            try {
                savedExecution.executionStatus = ExecutionStatus.valueOf((String) jsonMap.get(MilkConstantJson.CSTJSON_JOBEXECUTION_STATUS));
            } catch (Exception e) {
                // anormal
                savedExecution.executionStatus = ExecutionStatus.ERROR;
            }
            savedExecution.listEventSt = (String) jsonMap.get(MilkConstantJson.CSTJSON_JOBEXECUTION_LISTEVENTS_ST);
            savedExecution.reportInHtml = (String) jsonMap.get(MilkConstantJson.CSTJSON_JOBEXECUTION_REPORTINHTML);
            savedExecution.nbItemsProcessed = (Long) jsonMap.get(MilkConstantJson.CSTJSON_JOBEXECUTION_ITEMSPROCESSED);
            savedExecution.executionTimeInMs = (Long) jsonMap.get(MilkConstantJson.CSTJSON_JOBEXECUTION_TIMEINMS);
            savedExecution.hostName = (String) jsonMap.get(MilkConstantJson.CSTJSON_JOBEXECUTION_HOSTNAME);
            return savedExecution;
        }

        public String toString() {
            return executionStatus.toString() + " " + sdf.format(executionDate) + " " + reportInHtml;
        }
    }

    // save a job execution
    // keep a history of the last X execution. X is a parameters for each job.
    List<SavedExecution> listSavedExecution = new ArrayList<>();

    public SavedExecution registerNewExecution(Date currentDate, ExecutionStatus executionOperation, String hostName, String reportInHtml) {
        // update the trackExecution
        trackExecution.inExecutionHostName = hostName;
        trackExecution.startTime = currentDate.getTime();
        
        SavedExecution savedExecution = new SavedExecution(currentDate, executionOperation, hostName, reportInHtml, new ArrayList<BEvent>());

        addSavedExecution(savedExecution);
        return savedExecution;
    }

    public void replaceExecution(SavedExecution savedExecution, Date currentDate, MilkJobOutput output) {
        if (savedExecution != null)
            listSavedExecution.remove(savedExecution);
        registerExecution(currentDate, output);
    }
    public SavedExecution getCurrentExecution() {
        if (listSavedExecution.isEmpty())
            return null;
        return listSavedExecution.get(0);        
    }

    /**
     * register an execution. Keep the last 10
     * 
     * @param currentDate
     * @param output
     */
    public void registerExecution(Date currentDate, MilkJobOutput output) {
        trackExecution.lastExecutionDate = output.executionDate;
        trackExecution.lastExecutionStatus = output.executionStatus;

        // check the previous status
        boolean forceSaved = false;
        if (!listSavedExecution.isEmpty() && listSavedExecution.get(0).executionStatus == ExecutionStatus.BADCONFIGURATION)
            forceSaved = true;

        if ((!forceSaved) && SAVEDEXECUTIONPOLICY.SAVEONLYWORK.equals(savedExecutionPolicy)
                && (output.executionStatus == ExecutionStatus.SUCCESSNOTHING
                        || output.executionStatus == ExecutionStatus.NOEXECUTION)) {
            return; // no need to save it
        }

        SavedExecution savedExecution = new SavedExecution(output);
        addSavedExecution(savedExecution);

    }

    private void addSavedExecution(SavedExecution savedExecution) {
        listSavedExecution.add(0, savedExecution);

        if (listSavedExecution.size() > nbSavedExecution)
            listSavedExecution = listSavedExecution.subList(0, nbSavedExecution - 1);

        // Attention, limit the size of the saved Execution
        int estimationMaxItemsAccordingTheSize = estimateMaxItems(listSavedExecution);
        if (listSavedExecution.size() > estimationMaxItemsAccordingTheSize) {
            listSavedExecution = listSavedExecution.subList(0, estimationMaxItemsAccordingTheSize);
        }

    }

    /**
     * Mechanism to not saved too much data in the database. Limit the execution order according the result.
     */
    private final static int CST_MAXSIZE = 50000;

    private int estimateMaxItems(List<?> listOfItems) {
        if (listOfItems.size() <= 1)
            return listOfItems.size();
        String sizeSavedValue = JSONValue.toJSONString(listOfItems);
        if (sizeSavedValue.length() < CST_MAXSIZE)
            return listOfItems.size();
        // too big, let estimate how many data we can keep
        int averagePerItem = sizeSavedValue.length() / listOfItems.size();
        int maxItemsEstimate = CST_MAXSIZE / averagePerItem;
        return maxItemsEstimate > 1 ? maxItemsEstimate : 1;

    }
}
