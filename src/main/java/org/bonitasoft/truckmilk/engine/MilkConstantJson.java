package org.bonitasoft.truckmilk.engine;

/**
 * this class group all JSON constant
 * 
 */

public class MilkConstantJson {

    
    public static final String CSTJSON_PREFIX_HUMANREADABLE = "st";

    
    public static final String CSTJSON_TODAYTAGDAY ="todaytagday";
    
    /* ******************************************************************************** */
    /*                                                                                  */
    /* Milk Job */
    /*                                                                                  */
    /* ******************************************************************************** */

    public static final String CSTJSON_JOB_CRON = "cron";
    public static final String CSTJSON_SAVEDEXECUTIONPOLICY = "savedExecutionPolicy";
    public static final String CSTJSON_NB_SAVEDEXECUTION = "nbSavedExecution";
    public static final String CSTJSON_HAS_MEASUREMENT = "hasMeasurement";
    public static final String CSTJSON_NB_HISTORYMEASUREMENT = "nbHistoryMeasurement";
    public static final String CSTJSON_JOBCANBESTOPPED_ITEMS = "jobCanBeStoppedItems";
    public static final String CSTJSON_JOBCANBESTOPPED_DELAY = "jobCanBeStoppedDelay";
    public static final String CSTJSON_STOPAFTER_NBITEMS = "stopafterNbItems";
    public static final String CSTJSON_STOPAFTER_NBMINUTES = "stopafterNbMinutes";
    public static final String CSTJSON_HOSTSRESTRICTION = "hostsrestriction";
   
    
    public static final String CSTJSON_JOB_PLUGINNAME = "pluginname";
    public static final String CSTJSON_ENABLE = "enable";
    public static final String CSTJSON_PARAMETERS_DEF = "parametersdef";
    public static final String CSTJSON_ANALYSISDEF = "analysisdef";
    public static final String CSTJSON_PARAMETERS = "parameters";
    public static final String CSTJSON_PARAMETER_DELAY_SCOPE = "scope";
    public static final String CSTJSON_PARAMETER_DELAY_VALUE = "value";
    public static final String CSTJSON_JOB_PLUGINDISPLAYNAME = "plugindisplayname";
    public static final String CSTJSON_JOB_PLUGIN_WARNING = "pluginwarning";
    public static final String CSTJSON_JOB_PLUGIN_EXPLANATION = "pluginexplanation";
    public static final String CSTJSON_JOB_DESCRIPTION = "description";
    public static final String CSTJSON_JOB_NEXTEXECUTION = "nextexecution";
    public static final String CSTJSON_JOB_LASTEXECUTION = "lastexecution";
    public static final String CSTJSON_JOB_NAME = "name";
    public static final String CSTJSON_JOB_ID = "id";
    public static final String CSTJSON_JOB_IMMEDIATEEXECUTION = "imediateExecution";
    public static final String CSTJSON_JOB_ASKFORSTOP = "askForStop";
    public static final String CSTJSON_LAST_EXECUTION_STATUS = "lastexecutionstatus";
    public static final String CSTJSON_IN_EXECUTION = "inExecution";
    public static final String CSTJSON_INEXECUTION_HOST = "inExecutionHost";
    public static final String CSTJSON_INEXECUTION_IPADDRESS = "inExecutionIpAddress";
    public static final String CSTJSON_INEXECUTION_STARTTIME = "inExecutionStartTime";
    public static final String CSTJSON_INEXECUTION_PERCENT = "inExecutionPercent";
    public static final String CSTJSON_INEXECUTION_AVANCEMENTINFORMATION = "inExecutionInfo";    
    public static final String CSTJSON_INEXECUTION_ENDTIMEESTIMATED_MS = "inExecutionEndTimeEstimatedinMS";
    public static final String cstJsonInExecutionEndDateEstimated = "inExecutionEndDateEstimated";
    // saved last execution  
    public static final String CSTJSON_JOB_SAVEDEXEC = "savedExecution";
    public static final String CSTJSON_JOB_SAVEEXEC_OVERLOAD ="savedExecutionOverload";
    public static final String CSTJSON_JOB_SAVEDEXEC_MOREDATA = "savedExecutionMoreData";
    public static final String CSTJSON_JOB_TRACKEXECUTION = "trackExecution";
    
    public static final String CSTJSON_JOBEXECUTION_DATE = "execDate";
    public static final String CSTJSON_JOBEXECUTION_STATUS = "status";
    public static final String CSTJSON_JOBEXECUTION_TAGDAY = "tagDay";
    public static final String CSTJSON_JOBEXECUTION_LISTEVENTS_ST = "listevents";
    public static final String CSTJSON_JOBEXECUTION_REPORTINHTML = "reportinhtml";
    public static final String CSTJSON_JOBEXECUTION_ITEMSPROCESSED = "itemprocessed";
    public static final String CSTJSON_JOBEXECUTION_TIMEINMS = "timeinms";
    public static final String CSTJSON_JOBEXECUTION_HOSTNAME = "hostname";
    
    

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Measurement Job */
    /*                                                                                  */
    /* ******************************************************************************** */

    public static final String CSTJSON_MEASUREMENT_PLUGIN_NAME = "name";
    public static final String CSTJSON_MEASUREMENT_PLUGIN_LABEL = "label";
    public static final String CSTJSON_MEASUREMENT_PLUGIN_EXPLANATION= "explanation";
    public static final String CSTJSON_JOB_MEASUREMENT ="measurement";
    public static final String CSTJSON_MEASUREMENT_DEF = "def";
    public static final String CSTJSON_MEASUREMENT_HISTORY ="history";
    public static final String CSTJSON_MEASUREMENTATDATE_DATEST="datest";
    public static final String CSTJSON_MEASUREMENTATDATE_VALUEMAP="valuemap";
    public static final String CSTJSON_MEASUREMENTATDATE_VALUELIST="valuelist";

    public static final String CSTJSON_PLUGIN_DISPLAYNAME = "displayname";

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Plug In */
    /*                                                                                  */
    /* ******************************************************************************** */

    public static final String CSTJSON_PLUGIN_EXPLANATION = "pluginexplanation";
    public static final String CSTJSON_PLUGIN_EMBEDED = "embeded";
    public static final String CSTJSON_PLUGIN_LOCAL = "local";
    public static final String CSTJSON_PLUGIN_CMD = "cmd";
    public static final String CSTJSON_PLUGIN_CATEGORY = "category";
    public static final String CSTJSON_PLUGIN_TYPE = "type";

    public static final String CSTJSON_PLUGIN_PARAMETERNAME = "name";
    public static final String CSTJSON_PLUGIN_PARAMETERTYPE = "type";
    public static final String CSTJSON_PLUGIN_PARAMETERLABEL = "label";

    public static final String CSTJSON_PLUGIN_PARAMETERMANDATORY ="isMandatory";
    public static final String CSTJSON_PLUGIN_PARAMETERVISIBLECONDITION = "visibleCondition";
    public static final String CSTJSON_PLUGIN_PARAMETERFILTERPROCESS="filterProcess";

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Scheduler */
    /*                                                                                  */
    /* ******************************************************************************** */

    public static final String CSTJSON_DASHBOARD_EVENTS = "dashboardlistevents";
    public static final String cstJsonDashboardSyntheticEvents = "dashboardsyntheticlistevents";
    public static final String cstJsonScheduler = "scheduler";
    public static final String cstJsonSchedulerType = "type";
    public static final String CSTJSON_SCHEDULERINFO = "info";
    public static final String CSTJSON_LOGHEARTBEAT = "logheartbeat";
    public static final String CSTJSON_NBSAVEDHEARTBEAT = "nbsavedheartbeat";
 
    
    
    
    public static final String CSTJSON_SCHEDULER_STATUS = "status";
    public static final String CSTJSON_SCHEDULER_ISNEWSCHEDULERCHOOSEN = "isNewSchedulerChoosen";
    public static final String CSTJSON_SCHEDULER_ISSCHEDULERREADY = "isQuartzJob";
    public static final String CSTJSON_SCHEDULER_MESSAGE= "schedulerMessage";
    public static final String cstJsonSchedulerStatus_V_RUNNING = "RUNNING";
    public static final String cstJsonSchedulerStatus_V_SHUTDOWN = "SHUTDOWN";
    public static final String cstJsonSchedulerStatus_V_STOPPED = "STOPPED";

    public static final String cstSchedulerChangeType = "schedulerchangetype";
    public static final String cstJsonListTypesSchedulers = "listtypeschedulers";
    public static final String CSTJSON_LASTHEARTBEAT = "lastheartbeat";

    public static final String cstSchedulerOperation = "scheduleroperation";

    public static final String cstEnvironmentStatus = "ENVIRONMENTSTATUS";
    public static final String cstEnvironmentStatus_V_CORRECT = "OK";
    public static final String cstEnvironmentStatus_V_ERROR = "ERROR";
    
    
    public static final String CSTJSON_THREADDUMP = "threadDump";

}
