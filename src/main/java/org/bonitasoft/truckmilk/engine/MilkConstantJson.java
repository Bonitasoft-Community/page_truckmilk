package org.bonitasoft.truckmilk.engine;

import org.bonitasoft.truckmilk.engine.MilkPlugIn.TYPE_PLUGIN;

/**
 * this class group all JSON constant
 * 
 * @author Firstname Lastname
 */

public class MilkConstantJson {

    
    public final static String CSTJSON_PREFIX_HUMANREADABLE = "st";

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Milk Job */
    /*                                                                                  */
    /* ******************************************************************************** */

    public final static String CSTJSON_JOB_CRON = "cron";
    public final static String CSTJSON_SAVEDEXECUTIONPOLICY = "savedExecutionPolicy";
    public final static String CSTJSON_NB_SAVEDEXECUTION = "nbSavedExecution";
    public final static String CSTJSON_HAS_MEASUREMENT = "hasMeasurement";
    public final static String CSTJSON_NB_HISTORYMEASUREMENT = "nbHistoryMeasurement";
    public final static String CSTJSON_JOBCANBESTOPPED_ITEMS = "jobCanBeStoppedItems";
    public final static String CSTJSON_JOBCANBESTOPPED_DELAY = "jobCanBeStoppedDelay";
    public final static String CSTJSON_STOPAFTER_NBITEMS = "stopafterNbItems";
    public final static String CSTJSON_STOPAFTER_NBMINUTES = "stopafterNbMinutes";
    public final static String CSTJSON_HOSTSRESTRICTION = "hostsrestriction";
   
    
    public final static String CSTJSON_JOB_PLUGINNAME = "pluginname";
    public final static String CSTJSON_ENABLE = "enable";
    public final static String CSTJSON_PARAMETERS_DEF = "parametersdef";
    public final static String CSTJSON_ANALYSISDEF = "analysisdef";
    public final static String CSTJSON_PARAMETERS = "parameters";
    // private final static String cstJsonParameterValue = "value";
    public final static String CSTJSON_PARAMETER_DELAY_SCOPE = "scope";
    public final static String CSTJSON_PARAMETER_DELAY_VALUE = "value";
    public final static String CSTJSON_JOB_PLUGINDISPLAYNAME = "plugindisplayname";
    public final static String CSTJSON_JOB_PLUGIN_WARNING = "pluginwarning";
    public final static String CSTJSON_JOB_PLUGIN_EXPLANATION = "pluginexplanation";
    public final static String CSTJSON_JOB_DESCRIPTION = "description";
    public final static String CSTJSON_JOB_NEXTEXECUTION = "nextexecution";
    public final static String CSTJSON_JOB_LASTEXECUTION = "lastexecution";
    public final static String CSTJSON_JOB_NAME = "name";
    public final static String CSTJSON_JOB_ID = "id";
    public final static String CSTJSON_JOB_IMMEDIATEEXECUTION = "imediateExecution";
    public final static String cstJsonAskForStop = "askForStop";
    public final static String cstJsonlastExecutionStatus = "lastexecutionstatus";
    public final static String cstJsonInExecution = "inExecution";
    public final static String cstJsonRegisterInExecutionHost = "inExecutionHost";
    public final static String cstJsonInExecutionStartTime = "inExecutionStartTime";
    public final static String cstJsonInExecutionPercent = "inExecutionPercent";
    public final static String cstJsonInExecutionEndTimeEstimatedInMS = "inExecutionEndTimeEstimatedinMS";
    public final static String cstJsonInExecutionEndDateEstimated = "inExecutionEndDateEstimated";
    // saved last execution  
    public final static String CSTJSON_SAVEDEXEC = "savedExecution";
    public final static String CSTJSON_SAVEEXEC_OVERLOAD ="savedExecutionOverload";
    public final static String cstJsonSaveExecDate = "execDate";
    public final static String cstJsonSaveExecStatus = "status";
    public final static String cstJsonSaveExecListEventsSt = "listevents";
    public final static String CSTJSON_SAVEEXEC_REPORTINHTML = "reportinhtml";
    public final static String CSTJSON_SAVEEXEC_ITEMSPROCESSED = "itemprocessed";
    public final static String cstJsonSaveExecTimeinMs = "timeinms";
    public final static String cstJsonSaveExecHostName = "hostname";
    
    

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Measurement Job */
    /*                                                                                  */
    /* ******************************************************************************** */

    public final static String CSTJSON_MEASUREMENT_PLUGIN_NAME = "name";
    public final static String CSTJSON_MEASUREMENT_PLUGIN_LABEL = "label";
    public final static String CSTJSON_MEASUREMENT_PLUGIN_EXPLANATION= "explanation";
    public final static String CSTJSON_MEASUREMENT ="measurement";
    public final static String CSTJSON_MEASUREMENT_DEF = "def";
    public final static String CSTJSON_MEASUREMENT_HISTORY ="history";
    public final static String CSTJSON_MEASUREMENTATDATE_DATEST="datest";
    public final static String CSTJSON_MEASUREMENTATDATE_VALUEMAP="valuemap";
    public final static String CSTJSON_MEASUREMENTATDATE_VALUELIST="valuelist";

    public final static String CSTJSON_PLUGIN_DISPLAYNAME = "displayname";

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Plug In */
    /*                                                                                  */
    /* ******************************************************************************** */

    public final static String CSTJSON_PLUGIN_EXPLANATION = "pluginexplanation";
    public final static String CSTJSON_PLUGIN_EMBEDED = "embeded";
    public final static String CSTJSON_PLUGIN_LOCAL = "local";
    public final static String CSTJSON_PLUGIN_CMD = "cmd";
    public final static String CSTJSON_PLUGIN_CATEGORY = "category";
    public final static String CSTJSON_PLUGIN_TYPE = "type";

    public final static String CSTJSON_PLUGIN_PARAMETERNAME = "name";

    public final static String CSTJSON_PLUGIN_PARAMETERLABEL = "label";

    public final static String CSTJSON_PLUGIN_PARAMETERMANDATORY ="isMandatory";
    public final static String CSTJSON_PLUGIN_PARAMETERVISIBLECONDITION = "visibleCondition";


    /* ******************************************************************************** */
    /*                                                                                  */
    /* Scheduler */
    /*                                                                                  */
    /* ******************************************************************************** */

    public final static String cstJsonDashboardEvents = "dashboardlistevents";
    public final static String cstJsonDashboardSyntheticEvents = "dashboardsyntheticlistevents";
    public final static String cstJsonScheduler = "scheduler";
    public final static String cstJsonSchedulerType = "type";
    public final static String CSTJSON_SCHEDULERINFO = "info";
    public final static String CSTJSON_LOGHEARTBEAT = "logheartbeat";
    public final static String CSTJSON_NBSAVEDHEARTBEAT = "nbsavedheartbeat";
 
    
    
    
    public final static String cstJsonSchedulerStatus = "status";
    public final static String cstJsonSchedulerStatus_V_RUNNING = "RUNNING";
    public final static String cstJsonSchedulerStatus_V_SHUTDOWN = "SHUTDOWN";
    public final static String cstJsonSchedulerStatus_V_STOPPED = "STOPPED";

    public final static String cstSchedulerChangeType = "schedulerchangetype";
    public final static String cstJsonListTypesSchedulers = "listtypeschedulers";
    public final static String CSTJSON_LASTHEARTBEAT = "lastheartbeat";

    public final static String cstSchedulerOperation = "scheduleroperation";

    public final static String cstEnvironmentStatus = "ENVIRONMENTSTATUS";
    public final static String cstEnvironmentStatus_V_CORRECT = "OK";
    public final static String cstEnvironmentStatus_V_ERROR = "ERROR";

}
