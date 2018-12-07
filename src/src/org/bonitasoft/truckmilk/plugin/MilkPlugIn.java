package org.bonitasoft.truckmilk.plugin;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugTourInput;
import org.bonitasoft.truckmilk.toolbox.SendMail;
import org.bonitasoft.truckmilk.toolbox.TypesCast;
import org.bonitasoft.truckmilk.tour.MilkPlugInTour;
import org.json.simple.JSONValue;

/* ******************************************************************************** */
/*                                                                                  */
/* Plug in method */
/*                                                                                  */
/*
 * All plugin must extends this class.
 * Then, a new plugin will be deployed as a command in the BonitaEngine, in order
 * to be called on demand by the MilkCmdControl.
 * Register the embeded plug in in MilkCmdControl.detectListPlugIn()
 * Remark: MilkCmdControl is the command who control all plugin execution.
 * Multiple PlugTour can be define for each PlugIn
 * * Example: plugIn 'monitorEmail'
 * - plugTour "HumanRessourceEmail" / Every day / Monitor 'hr@bonitasoft.com'
 * - plugTour "SaleEmail" / Every hour / Monitor 'sale@bonitasoft.com'
 * A plug in may be a command or may be embeded.
 * So:
 * - MilkCmdControl can do a MutiThread call on the method Execute()
 * ==> Object must not save information as static
 * - MilkCmdControl will set all parameters on the execution() and the execution
 * must return a synthetic value
 * ==> consider the execute() as a static;
 */
/*                                                                                  */
/*                                                                                  */
/* ******************************************************************************** */

public abstract class MilkPlugIn {


  public static BEvent EVENT_UNKNOW_BUTTON = new BEvent(MilkPlugIn.class.getName(), 1,
      Level.ERROR,
      "Unknow button", "A button is executed, but no action is known for this button", "No action executed.",
      "Reference the error");

  
  /* ******************************************************************************** */
  /*                                                                                  */
  /* Definition */
  /*                                                                                  */
  /*                                                                                  */
  /* ******************************************************************************** */

  public MilkPlugIn() {
  }

  /**
   * an embeded plugin means the plugin is deliver inside the Miltour library. Then, the
   * MilkCmdControl does not need to use the Command communication
   * 
   * @return
   */
  public boolean isEmbeded() {
    return false;
  };

  public abstract List<BEvent> checkEnvironment(long tenantId);

  public List<BEvent> initialize(Long tenantId) {
    List<BEvent> listEvents = new ArrayList<BEvent>();

    // if isEmbeded, just get the description and save it localy
    if (isEmbeded())
      description = getDescription();
    else {
      // collect the description by calling the command

      // and then get it localy
    }
    return listEvents;
  }

  /**
   * PROCESSNAME : user will have a autocomplete list to select a process + version in existing
   * process
   * ARRAY : an array on String
   * ARRAYPROCESSNAME : an array on processName + version
   * ARRAYMAP : then the arrayMapDescription has to be given
   */
  public enum TypeParameter {
    USERNAME, PROCESSNAME, STRING, TEXT, LONG, OBJECT, ARRAY, ARRAYPROCESS, BOOLEAN, ARRAYMAP, BUTTONARGS
  };

  public static class ColDefinition {

    public String name;
    public String title;
    public String tips;
    public int length = 30;
    public TypeParameter typeParameter;

    public static ColDefinition getInstance(String name, String title, String tips, TypeParameter typeParameter, int length) {
      ColDefinition colDefinition = new ColDefinition();
      colDefinition.name = name;
      colDefinition.title = title;
      colDefinition.tips = tips;
      colDefinition.typeParameter = typeParameter;
      colDefinition.length = length;
      return colDefinition;
    }

    public Map<String, Object> getMap() {
      Map<String, Object> map = new HashMap<String, Object>();

      map.put("name", name);
      map.put("title", title);
      map.put("tips", tips);
      map.put("typeParameter", typeParameter.toString());
      map.put("length", length);
      return map;
    }
  }

  public static class PlugInParameter {

    public String name;
    public Object defaultValue;
    public String explanation;
    public TypeParameter typeParameter;

    // a button have args : give it the number of args 
    public String buttonDescription;
    public List<String> argsName;
    public List<String> argsValue;
    
    public List<ColDefinition> arrayMapDescription;

    public static PlugInParameter createInstance(String name, TypeParameter typeParameter, Object defaultValue, String explanation) {
      PlugInParameter plugInParameter = new PlugInParameter();
      plugInParameter.name = name;
      plugInParameter.typeParameter = typeParameter;
      plugInParameter.defaultValue = defaultValue;
      plugInParameter.explanation = explanation;
      // set an empty value then
      if (typeParameter == TypeParameter.ARRAY && defaultValue == null)
        plugInParameter.defaultValue = new ArrayList<Object>();

      return plugInParameter;
    }

    public static PlugInParameter createInstanceArrayMap(String name, List<ColDefinition> arrayMapDefinition, Object defaultValue, String explanation) {
      PlugInParameter plugInParameter = new PlugInParameter();
      plugInParameter.name = name;
      plugInParameter.typeParameter = TypeParameter.ARRAYMAP;
      plugInParameter.arrayMapDescription = arrayMapDefinition;
      plugInParameter.defaultValue = defaultValue;
      plugInParameter.explanation = explanation;
      // set an empty value then
      if (defaultValue == null)
        plugInParameter.defaultValue = new ArrayList<Object>();

      return plugInParameter;
    }
    
    /**
     * create a button Args paameters. It's possible to have multiple default value (if the number of args is upper than 1). Give the list of default value separate by a <->
     * Example with 3 args : "this is the default first args<->and the second<->the third"
     * @param name
     * @param buttonNbArgs
     * @param defaultValue
     * @param explanation
     * @return
     */
    public static PlugInParameter createInstanceButton(String name,  List<String> argsName, List<String> argsValue, String buttonDescription, String explanation) {
      PlugInParameter plugInParameter = new PlugInParameter();
      plugInParameter.name = name;
      plugInParameter.typeParameter = TypeParameter.BUTTONARGS;
      plugInParameter.argsName= argsName;
      plugInParameter.argsValue= argsValue;
      plugInParameter.buttonDescription = buttonDescription;
      plugInParameter.explanation = explanation;
      return plugInParameter;
    }
    /**
     * here the JSON returned to the HTML to build the parameters display
     * @return
     */
    public Map<String, Object> getMap() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("name", name);
      map.put("type", typeParameter.toString());
      if (arrayMapDescription != null) {
        List<Map<String, Object>> listDescription = new ArrayList<Map<String, Object>>();
        for (ColDefinition col : arrayMapDescription) {
          listDescription.add(col.getMap());
        }
        map.put("arraymapDefinition", listDescription);
      }
      if (MilkPlugIn.TypeParameter.BUTTONARGS.equals( typeParameter))
      {
        List<Map<String, Object>> listArgs = new ArrayList<Map<String, Object>>();
        if (argsName!=null)
          for (int i=0;i<argsName.size();i++) {
            Map<String, Object> mapArgs = new HashMap<String, Object>();
            mapArgs.put("name", i < argsName.size()? argsName.get( i ) : "");
            mapArgs.put("value", i < argsValue.size()? argsValue.get( i ) : "");
            
            listArgs.add( mapArgs );
          }
        map.put("args", listArgs);
        map.put("buttonDescription", buttonDescription);
      }
        
      map.put("explanation", explanation);
      map.put("defaultValue", defaultValue);
      return map;
    }
  }

  public static class PlugInDescription {

    public String name;
    public String version;
    public String displayName;
    /**
     * description is give by the user, to override it
     */
    public String description;
    /**
     * explanatino is given by the plug in, user can't change it
     */
    public String explanation;
    public List<PlugInParameter> inputParameters = new ArrayList<PlugInParameter>();
    public String cronSt = "0 0/10 * 1/1 * ? *"; // every 10 mn

    public void addParameter(PlugInParameter parameter) {
      inputParameters.add(parameter);
    }

    /**
     * Expected format :
     * "{\"delayinmn\":10,\"maxtentative\":12,\"processfilter\":[{\"name\":\"expens*\",\"version\":null}]}")
     * 
     * @param jsonSt
     */
    @SuppressWarnings("unchecked")
    public void addParameterFromMapJson(String jsonSt) {
      Map<String, Object> mapJson = (Map<String, Object>) JSONValue.parse(jsonSt);
      for (String key : mapJson.keySet())
        addParameter(PlugInParameter.createInstance(key, TypeParameter.STRING, mapJson.get(key), null));
    }

    public Map<String, Object> getParametersMap() {
      Map<String, Object> map = new HashMap<String, Object>();
      for (PlugInParameter plugInParameter : inputParameters) {
        map.put(plugInParameter.name, plugInParameter.defaultValue);
      }
      return map;
    }

  }

  private PlugInDescription description;

  public final PlugInDescription getLocalDescription() {
    return description;
  }

  /**
   * get the name, should be unique in all plugin
   * 
   * @return
   */
  public String getName() {
    return (description == null ? this.getClass().getName() : description.name);
  }

  /**
   * describe the plug in
   * 
   * @return
   */
  public Map<String, Object> getMap() {
    Map<String, Object> result = new HashMap<String, Object>();
    result.put("name", description == null ? null : description.name);
    result.put("displayname", description == null ? null : description.displayName);
    result.put("description", description == null ? null : description.description);
    result.put("embeded", isEmbeded());

    return result;

  }
  /* ******************************************************************************** */
  /*                                                                                  */
  /* Execution */
  /*                                                                                  */
  /*                                                                                  */
  /* ******************************************************************************** */

  public static class PlugTourInput {

    public String tourName;
    // this parameters is initialised with the value in the PlugTour, and may change after
    public Map<String, Object> tourParameters;

    private MilkPlugInTour milkPlugInTour;

    public PlugTourInput(MilkPlugInTour milkPlugInTour) {
      this.milkPlugInTour = milkPlugInTour;
      this.tourParameters = milkPlugInTour.getTourParameters();

    }

    public PlugTourOutput getPlugTourOutput() {
      return new PlugTourOutput(milkPlugInTour);
    }

    /**
     * Boolean
     * 
     * @param parameter
     * @return
     */
    public Boolean getInputBooleanParameter(PlugInParameter parameter) {
      return getInputBooleanParameter(parameter.name, (Boolean) parameter.defaultValue);
    }

    public Boolean getInputBooleanParameter(String name, Boolean defaultValue) {
      return TypesCast.getBoolean(tourParameters.get(name).toString(), defaultValue);

    }

    /**
     * Long
     * 
     * @param parameter
     * @return
     */
    public Long getInputLongParameter(PlugInParameter parameter) {
      return getInputLongParameter(parameter.name, parameter.defaultValue == null ? null : Long.valueOf(parameter.defaultValue.toString()));
    }

    public Long getInputLongParameter(String name, Long defaultValue) {
      return TypesCast.getLong(tourParameters.get(name), defaultValue);
    }

    public String getInputStringParameter(PlugInParameter parameter) {
      return getInputStringParameter(parameter.name, (String) parameter.defaultValue);
    }

    public String getInputStringParameter(String name, String defaultValue) {
      return TypesCast.getString(tourParameters.get(name), defaultValue);
    }

    public List<?> getInputListParameter(PlugInParameter parameter) {
      return getInputListParameter(parameter.name, (List<?>) parameter.defaultValue);
    }

    public List<?> getInputListParameter(String name, List<?> defaultValue) {
      try {
        return (List<?>) tourParameters.get(name);
      } catch (Exception e) {
        return defaultValue;
      }
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> getInputListMapParameter(PlugInParameter parameter) {
      return (List<Map<String, Object>> ) getInputListParameter(parameter.name, (List<Map<String, Object>>) parameter.defaultValue);
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> getInputListMapParameter(String name, List<Map<String, Object>> defaultValue) {
      try {
        return (List<Map<String, Object>>) tourParameters.get(name);
      } catch (Exception e) {
        return defaultValue;
      }
    }

  }

  public enum ExecutionStatus {
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

  public static class PlugTourOutput {

    /**
     * main result of the execution is a list of Events and the status.
     * Null until a new execution is started
     */
    private List<BEvent> listEvents = null;

    // the listEvents is saved in the St
    public String listEventsSt = "";
    /**
     * give a simple status execution.
     */
    public ExecutionStatus executionStatus = ExecutionStatus.NOEXECUTION;

    /** save the time need to the execution */
    public Long executionTimeInMs;

    public MilkPlugInTour plugInTour;

    public String explanation;

    // if you have a PlugInTourInput, create the object from this
    public PlugTourOutput(MilkPlugInTour plugInTour) {
      this.plugInTour = plugInTour;
    }

    public void addEvent(BEvent event) {
      // new execution : create the list event to collect it
      if (listEvents == null)
        listEvents = new ArrayList<BEvent>();
      listEvents.add(event);
    }

    public void addEvents(List<BEvent> events) {
      // new execution : create the list event to collect it
      if (listEvents == null)
        listEvents = new ArrayList<BEvent>();
      listEvents.addAll(events);
    }

    public List<BEvent> getListEvents() {
      return listEvents;
    }
  }

  /* ******************************************************************************** */
  /*                                                                                  */
  /* Abstract method */
  /*                                                                                  */
  /*                                                                                  */
  /* ******************************************************************************** */

  /**
   * return a unique description
   * 
   * @return
   */
  public abstract PlugInDescription getDescription();

  public abstract PlugTourOutput execute(PlugTourInput input, APIAccessor apiAccessor);

  /** this method can be override by the plug in if it create a button "BUTTONPARAMETERS"
   * 
   * @param parameters
   * @return
   */
  public List<BEvent> buttonParameters(String buttonName, PlugTourInput input, Map<String, Object> argsParameters, APIAccessor apiAccessor)
  {
    return new ArrayList<BEvent>();
  }
  
  /* ******************************************************************************** */
  /*                                                                                  */
  /* Tool for plug in */
  /*                                                                                  */
  /*                                                                                  */
  /* ******************************************************************************** */

  public List<BEvent> createCase() {
    List<BEvent> listEvents = new ArrayList<BEvent>();
    return listEvents;
  }

  public List<BEvent> executeTasks() {
    List<BEvent> listEvents = new ArrayList<BEvent>();
    return listEvents;

  }
}
