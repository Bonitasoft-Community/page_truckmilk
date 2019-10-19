package org.bonitasoft.truckmilk.engine;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.json.simple.JSONValue;

/* ******************************************************************************** */
/*                                                                                  */
/* Plug in method */
/*                                                                                  */
/*
 * All plugin must extends this class.
 * Then, a new plugin will be deployed as a command in the BonitaEngine, in order
 * to be called on demand by the MilkCmdControl.
 * 
 * Register the embeded plug in in MilkPlugInFactory.collectListPlugIn()
 * 
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

    public static Logger logger = Logger.getLogger(MilkPlugIn.class.getName());

    /**
     * the base keep the description available.
     * To be sure this is correclty initialised, the factoryPlugIn, who create object, call the
     * initialise after
     */
    private PlugInDescription description;

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Definition */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    /*
     * embedded: the plug in is given with the truck milk
     * local : the plug in is loaded in the BonitaServer, as a dependence, via the truck milk page
     * command : the plug is is an external Command Call, then it must be deployed as a command, and
     * can be only available on some host (cluster mode)
     */

    public enum TYPE_PLUGIN {
        EMBEDED, LOCAL, COMMAND
    };

    private TYPE_PLUGIN typePlugIn;

    /**
     * then only the factory can create an object
     * Default Constructor.
     */
    protected MilkPlugIn(TYPE_PLUGIN typePlugIn) {
        this.typePlugIn = typePlugIn;
    }

    public PlugInDescription getDescription() {
        return description;
    }

    /**
     * an embeded plugin means the plugin is deliver inside the Miltour library. Then, the
     * MilkCmdControl does not need to use the Command communication
     * 
     * @return
     */
    public TYPE_PLUGIN getTypePlugin() {
        return typePlugIn;
    };

    public abstract List<BEvent> checkEnvironment(long tenantId, APIAccessor apiAccessor);

    /**
     * Initialise. This method is call by the Factory, after the object is created.
     * 
     * @param tenantId
     * @return
     */
    protected List<BEvent> initialize(Long tenantId) {
        List<BEvent> listEvents = new ArrayList<BEvent>();

        // if isEmbeded, just get the description and save it localy
        if (typePlugIn == TYPE_PLUGIN.EMBEDED || typePlugIn == TYPE_PLUGIN.LOCAL)
            description = getDefinitionDescription();
        else if (typePlugIn == TYPE_PLUGIN.COMMAND) {
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
     * FILEREAD : the plug in access this parameters in READ ONLY. Administrator is suppose to give
     * the file then.
     * FILEWRITE: the plug in write the file. Administrator can download it only
     * FILEREADWRITE : plug in and administrator can read and write the file
     */
    public enum TypeParameter {
        USERNAME, PROCESSNAME, STRING, TEXT, JSON, LONG, OBJECT, ARRAY, ARRAYPROCESS, BOOLEAN, ARRAYMAP, BUTTONARGS, FILEREAD, FILEWRITE, FILEREADWRITE
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
        public String label;
        public Object defaultValue;
        public String explanation;
        public TypeParameter typeParameter;

        // a button have args : give it the number of args 
        public String buttonDescription;
        public List<String> argsName;
        public List<String> argsValue;

        // a FILEWRITE, FILEREAD, FILEREADWRITE has a content type and a file name
        public String fileName;
        public String contentType;

        public List<ColDefinition> arrayMapDescription;

        public static PlugInParameter createInstance(String name, String label, TypeParameter typeParameter, Object defaultValue, String explanation) {
            PlugInParameter plugInParameter = new PlugInParameter();
            plugInParameter.name = name;
            plugInParameter.label = label;
            plugInParameter.typeParameter = typeParameter;
            plugInParameter.defaultValue = defaultValue;
            plugInParameter.explanation = explanation;
            // set an empty value then
            if (typeParameter == TypeParameter.ARRAY && defaultValue == null)
                plugInParameter.defaultValue = new ArrayList<Object>();

            return plugInParameter;
        }

        public static PlugInParameter createInstanceArrayMap(String name, String label, List<ColDefinition> arrayMapDefinition, Object defaultValue, String explanation) {
            PlugInParameter plugInParameter = new PlugInParameter();
            plugInParameter.name = name;
            plugInParameter.label = label;
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
         * create a button Args paameters. It's possible to have multiple default value (if the number
         * of args is upper than 1). Give the list of default value separate by a <->
         * Example with 3 args : "this is the default first args<->and the second<->the third"
         * 
         * @param name
         * @param buttonNbArgs
         * @param defaultValue
         * @param explanation
         * @return
         */
        public static PlugInParameter createInstanceButton(String name, String label, List<String> argsName, List<String> argsValue, String buttonDescription, String explanation) {
            PlugInParameter plugInParameter = new PlugInParameter();
            plugInParameter.name = name;
            plugInParameter.label = label;
            plugInParameter.typeParameter = TypeParameter.BUTTONARGS;
            plugInParameter.argsName = argsName;
            plugInParameter.argsValue = argsValue;
            plugInParameter.buttonDescription = buttonDescription;
            plugInParameter.explanation = explanation;
            return plugInParameter;
        }

        public static PlugInParameter createInstanceFile(String name, String label, TypeParameter typeParameter, Object defaultValue, String explanation, String fileName, String contentType) {
            PlugInParameter plugInParameter = new PlugInParameter();
            plugInParameter.name = name;
            plugInParameter.label = label;
            plugInParameter.typeParameter = typeParameter;
            plugInParameter.defaultValue = defaultValue;
            plugInParameter.explanation = explanation;
            plugInParameter.fileName = fileName;
            plugInParameter.contentType = contentType;

            // set an empty value then
            if (typeParameter == TypeParameter.ARRAY && defaultValue == null)
                plugInParameter.defaultValue = new ArrayList<Object>();

            return plugInParameter;
        }

        public static String cstJsonParameterName = "name";
        public static String cstJsonParameterLabel = "label";

        /**
         * here the JSON returned to the HTML to build the parameters display
         * 
         * @return
         */
        public Map<String, Object> getMap() {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put(cstJsonParameterName, name);
            map.put(cstJsonParameterLabel, label);

            map.put("type", typeParameter.toString());
            if (arrayMapDescription != null) {
                List<Map<String, Object>> listDescription = new ArrayList<Map<String, Object>>();
                for (ColDefinition col : arrayMapDescription) {
                    listDescription.add(col.getMap());
                }
                map.put("arraymapDefinition", listDescription);
            }
            if (MilkPlugIn.TypeParameter.BUTTONARGS.equals(typeParameter)) {
                List<Map<String, Object>> listArgs = new ArrayList<Map<String, Object>>();
                if (argsName != null)
                    for (int i = 0; i < argsName.size(); i++) {
                        Map<String, Object> mapArgs = new HashMap<String, Object>();
                        mapArgs.put("name", i < argsName.size() ? argsName.get(i) : "");
                        mapArgs.put("value", i < argsValue.size() ? argsValue.get(i) : "");

                        listArgs.add(mapArgs);
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
        public String label;
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
                Object label = (String) mapJson.get(key + "_label");
                if (label == null)
                    label = key;
                addParameter(PlugInParameter.createInstance(key, label.toString(), TypeParameter.STRING, mapJson.get(key), null));
            }
        }

        public Map<String, Object> getParametersMap() {
            Map<String, Object> map = new HashMap<String, Object>();
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
        result.put("displayname", description == null ? null : description.label);
        result.put("description", description == null ? null : description.description);
        result.put("embeded", typePlugIn == TYPE_PLUGIN.EMBEDED);
        result.put("local", typePlugIn == TYPE_PLUGIN.LOCAL);
        result.put("cmd", typePlugIn == TYPE_PLUGIN.COMMAND);
        result.put("type", typePlugIn.toString());

        return result;

    }

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

    public static class PlugTourOutput {

        public Date executionDate = new Date();
        /**
         * main result of the execution is a list of Events and the status.
         * Null until a new execution is started
         */
        private List<BEvent> listEvents = new ArrayList<BEvent>();

        // the listEvents is saved in the St
        public String listEventsSt = "";
        /**
         * give a simple status execution.
         */
        public ExecutionStatus executionStatus = ExecutionStatus.NOEXECUTION;

        /** save the time need to the execution */
        public Long executionTimeInMs;

        public MilkJob plugInTour;

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
        public PlugTourOutput(MilkJob plugInTour) {
            this.plugInTour = plugInTour;
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

        public void setParameterStream(PlugInParameter param, InputStream stream) {
            if (param.typeParameter == TypeParameter.FILEWRITE || param.typeParameter == TypeParameter.FILEREADWRITE) {
                // update the PLUGIN parameters
                plugInTour.setParameterStream(param, stream);
            } else {
                logger.severe("setParameterStream not allowed on parameter[" + param.name + "] (plugin " + plugInTour.getName() + "]");
            }
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
    public abstract PlugInDescription getDefinitionDescription();

    public abstract PlugTourOutput execute(MilkJobExecution input, APIAccessor apiAccessor);

    /**
     * this method can be override by the plug in if it create a button "BUTTONPARAMETERS"
     * 
     * @param parameters
     * @return
     */
    public List<BEvent> buttonParameters(String buttonName, MilkJobExecution input, Map<String, Object> argsParameters, APIAccessor apiAccessor) {
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
