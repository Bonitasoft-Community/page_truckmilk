package org.bonitasoft.truckmilk.tour;

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
import org.bonitasoft.truckmilk.plugin.MilkPlugIn;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.ExecutionStatus;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugInDescription;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugTourOutput;

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

public class MilkPlugInTour {

  public static Logger logger = Logger.getLogger(MilkPlugInTour.class.getName());
  private static BEvent eventCronParseError = new BEvent(MilkPlugInTour.class.getName(), 1, Level.APPLICATIONERROR,
      "Bad cron expression ", "Cron expression is not correct", "The next date can't be calculated",
      "Give a correct date");

  public static String DEFAULT_NAME = "";
  /**
   * the reference to the object to execute the Tour
   */
  public MilkPlugIn plugIn;

  /**
   * be compatible : old TruckMilk does not have Id
   */
  public long id = 0;
  /** keep the mark, then we can save again immediately if the tour didn't have an ID */
  public boolean newIdGenerated = false;

  public String name;
  public String description;

  public boolean isEnable = false;

  public boolean isImmediateExecution = false;

  public String cronSt = "";
  public Date nextExecutionDate;


  /** keep the last Execution Date and Status, for the dashboard
   * 
   */
  public Date lastExecutionDate;
  public ExecutionStatus lastExecutionStatus;
  
  
  public Map<String, Object> parameters = new HashMap<String, Object>();

  public MilkPlugInTour(String name, MilkPlugIn plugIn) {
    this.plugIn = plugIn;
    this.name = name == null ? MilkPlugInTour.DEFAULT_NAME : name;
  }

  public static MilkPlugInTour getInstanceFromPlugin(String name, MilkPlugIn plugIn) {
    MilkPlugInTour milkPlugInTour = new MilkPlugInTour(name, plugIn);
    PlugInDescription description = plugIn.getLocalDescription();
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
    return id;
  }

  public String getName() {
    return name == null ? MilkPlugInTour.DEFAULT_NAME : name;
  };

  public void setName(String name) {
    if (name == null)
      name = MilkPlugInTour.DEFAULT_NAME;
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
   * Operation on tour
   * /*
   */
  /* ******************************************************************************** */
  public List<BEvent> checkByPlugIn() {
    List<BEvent> listEvents = new ArrayList<BEvent>();
    if (parameters == null)
      parameters = new HashMap<String, Object>();
    // verify that all plugin parameters are in
    for (PlugInParameter inputparameters : plugIn.getDescription().inputParameters) {
      if (!parameters.containsKey(inputparameters.name)) {
        // new parameters
        parameters.put(inputparameters.name, inputparameters.defaultValue);
      }
    }
    return listEvents;
  }

  /* ******************************************************************************** */
  /*                                                                                  */
  /*
   * Operation on tour
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
   * get the parameters for this tour
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
    this.parameters = parameters ==null ? new HashMap<String,Object>() : parameters;
  }

  /* ******************************************************************************** */
  /*                                                                                  */
  /* Save / Load */
  /*                                                                                  */
  /* ******************************************************************************** */
  private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  /**
   * describe the plug tour
   * 
   * @return
   */
  private final static String cstJsonPluginName = "pluginname";
  private final static String cstJsonEnable = "enable";
  private final static String cstJsonParameters = "parameters";
  private final static String cstJsonParametersDef = "parametersdef";
  private final static String cstJsonPlugInDisplayName = "plugindisplayname";
  private final static String cstJsonDescription = "description";
  private final static String cstJsonExplanation = "explanation";
  private final static String cstJsonCron = "cron";
  private final static String cstJsonNextExecution = "nextexecution";
  private final static String cstJsonLastExecution = "lastexecution";
  private final static String cstJsonName = "name";
  private final static String cstJsonId = "id";
  private final static String cstJsonImmediateExecution = "imediateExecution";
  private final static String cstJsonlastExecutionStatus="lastexecutionstatus";
  
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
   * @param jsonSt
   * @param milkCmdControl
   * @return
   */
  
  public static MilkPlugInTour getInstanceFromJson(String jsonSt, MilkCmdControl milkCmdControl) {
    Map<String, Object> jsonMap = (Map<String, Object>) JSONValue.parse(jsonSt);
    if (jsonMap == null)
      return null;

    String plugInName = (String) jsonMap.get(cstJsonPluginName);
    MilkPlugIn plugIn = milkCmdControl.getPluginFromName(plugInName);
    if (plugIn == null)
      return null;

    String name = (String) jsonMap.get(cstJsonName);
    MilkPlugInTour plugInTour = new MilkPlugInTour(name, plugIn);
    plugInTour.description = (String) jsonMap.get(cstJsonDescription);

    plugInTour.id = getLongValue(jsonMap.get(cstJsonId), 0L);
    plugInTour.checkId();

    // clone the parameters !
    // new HashMap<>(description.getParametersMap()) not need at this moment because the maps is created
    plugInTour.parameters = (Map<String, Object>) jsonMap.get(cstJsonParameters);
    if (plugInTour.parameters == null)
      plugInTour.parameters = new HashMap<String, Object>();

    plugInTour.cronSt = (String) jsonMap.get(cstJsonCron);
    // search the name if all the list
    plugInTour.isEnable = getBooleanValue(jsonMap.get(cstJsonEnable), false);
    plugInTour.isImmediateExecution = getBooleanValue(jsonMap.get(cstJsonImmediateExecution), false);
    Long nextExecutionDateLong = (Long) jsonMap.get(cstJsonNextExecution);
    if (nextExecutionDateLong != null && nextExecutionDateLong !=0)
      plugInTour.nextExecutionDate = new Date(nextExecutionDateLong);

    Long lastExecutionDateLong = (Long) jsonMap.get(cstJsonLastExecution);
    if (lastExecutionDateLong != null && lastExecutionDateLong !=0)
      plugInTour.lastExecutionDate = new Date(lastExecutionDateLong);

    String lastExecutionStatus = (String) jsonMap.get(cstJsonlastExecutionStatus);
    if (lastExecutionStatus != null)
      plugInTour.lastExecutionStatus = ExecutionStatus.valueOf( lastExecutionStatus );
    

    if (plugInTour.isEnable && plugInTour.nextExecutionDate == null)
      plugInTour.calculateNextExecution();

    // get the last saved execution
    List<Map<String,Object>> list = (List) jsonMap.get(cstJsonSavedExec);
    if (list!=null)
    {
      for (Map<String,Object> execSaveMap : list)
      {
        plugInTour.listSavedExecution.add( SavedExecution.getInstance(execSaveMap));
      }
    }
    return plugInTour;
  }

  /**
   * getMap : use to save it or send to the browser
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
      map.put(cstJsonPlugInDisplayName, plugIn.getDescription().displayName);
    }
    map.put(cstJsonCron, cronSt);    
    map.put(cstJsonParameters, parameters );
    // create the list of parameters definition
    List<Map<String, Object>> listParametersDef = new ArrayList<Map<String, Object>>();
    map.put(cstJsonParametersDef, listParametersDef);
    for (PlugInParameter inputParameters : plugIn.getDescription().inputParameters) {
      Map<String, Object> mapParameter = inputParameters.getMap();
      mapParameter.put("value", parameters.get(inputParameters.name));
      listParametersDef.add(mapParameter);
    }

    if (isEnable) {
      map.put(cstJsonNextExecution, nextExecutionDate == null ? 0 : nextExecutionDate.getTime());
      map.put("nextexecutionst", nextExecutionDate == null ? "" : sdf.format(nextExecutionDate));
    }

    map.put(cstJsonLastExecution, lastExecutionDate == null ? 0 : lastExecutionDate.getTime());
    map.put("lastexecutionst", lastExecutionDate == null ? "" : sdf.format(lastExecutionDate));
    map.put(cstJsonlastExecutionStatus, lastExecutionStatus==null ? null : lastExecutionStatus.toString() );

    
    map.put(cstJsonEnable, isEnable);
    map.put(cstJsonImmediateExecution, isImmediateExecution);

    
    // save the last execution
    List<Map<String,Object>> listExecution = new ArrayList<Map<String,Object>>();
    for (SavedExecution savedExecution : listSavedExecution)
    {
      listExecution.add( savedExecution.getMap());
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
    if (id == 0) {
      // sleep a little to be sure to have a unique ID in case of a loop
      newIdGenerated = true;
      try {
        Thread.sleep(2);
      } catch (InterruptedException e) {
      }
      id = System.currentTimeMillis();
    }
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
    public long nbItemsProcessed=0;
    public long executionTimeInMs;
    public SavedExecution()
    {};
    
    public SavedExecution(PlugTourOutput output )
    {
      executionDate         = output.executionDate;
      executionStatus       = output.executionStatus;
      listEventSt           = BEventFactory.getSyntheticHtml(output.getListEvents());
      explanation           = output.explanation;
      nbItemsProcessed      = output.nbItemsProcessed;
      executionTimeInMs     = output.executionTimeInMs;
    }
    public Map<String,Object> getMap()
    {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put(cstJsonSaveExecDate, executionDate.getTime());
      map.put(cstJsonSaveExecDate+"St", sdf.format( executionDate));
      map.put(cstJsonSaveExecStatus, executionStatus.toString());
      map.put(cstJsonSaveExecListEventsSt, listEventSt);
      map.put(cstJsonSaveExecExplanation, explanation);
      map.put(cstJsonSaveExecItemsProcessed, nbItemsProcessed);
      map.put(cstJsonSaveExecTimeinMs, executionTimeInMs);
      
      return map;
    }
    public static SavedExecution getInstance( Map<String,Object> jsonMap )
    {
      SavedExecution savedExecution = new SavedExecution();
      
      Long execDateLong = (Long) jsonMap.get(cstJsonSaveExecDate);
      if (execDateLong != null && execDateLong !=0)
        savedExecution.executionDate  = new Date(execDateLong);
      savedExecution.executionStatus    = ExecutionStatus.valueOf( (String) jsonMap.get(cstJsonSaveExecStatus ) );
      savedExecution.listEventSt        =  (String) jsonMap.get(cstJsonSaveExecListEventsSt);
      savedExecution.explanation        =  (String) jsonMap.get(cstJsonSaveExecExplanation);
      savedExecution.nbItemsProcessed   =  (Long) jsonMap.get(cstJsonSaveExecItemsProcessed);
      savedExecution.executionTimeInMs  =  (Long) jsonMap.get(cstJsonSaveExecTimeinMs);

      return savedExecution;
    }
  }
  
  // save a tour execution
  List<SavedExecution> listSavedExecution = new ArrayList<SavedExecution>();
  
  /**
   * register an execution. Keep the last 10 
   * @param currentDate
   * @param output
   */
  public void registerExecution(Date currentDate, PlugTourOutput output) {
    lastExecutionDate = output.executionDate;
    lastExecutionStatus = output.executionStatus;
    if (output.executionStatus == ExecutionStatus.SUCCESSNOTHING
        || output.executionStatus == ExecutionStatus.NOEXECUTION)
    {
      return; // no need to save it
    }
    
    SavedExecution savedExecution = new SavedExecution(output);
    listSavedExecution.add(0, savedExecution);
    if (listSavedExecution.size() > 10)
      listSavedExecution.remove(10);
  }
}
