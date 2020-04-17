package org.bonitasoft.truckmilk.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

import lombok.Data;

/* ******************************************************************************** */
/*                                                                                  */
/* Plug in method */
/*                                                                                  */
/*
 * All plugin must extends this class.
 * Then, a new plugin will be deployed as a command in the BonitaEngine, in order
 * to be called on demand by the MilkCmdControl.
 * -------------------------- IMPORTANT
 * Register the embeded plug in in MilkPlugInFactory.collectListPlugIn()
 * Remark: MilkCmdControl is the command who control all plugin execution.
 * --------------------------------
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

    public final static BEvent eventUnknowButton = new BEvent(MilkPlugIn.class.getName(), 1,
            Level.ERROR,
            "Unknow button", "A button is executed, but no action is known for this button", "No action executed.",
            "Check error");
    public final static BEvent eventExecutionError = new BEvent(MilkPlugIn.class.getName(), 2,
            Level.ERROR,
            "Error during plugin execution", "An error is detected", "No action executed.",
            "Check error");

    private final static MilkLog logger = MilkLog.getLogger(MilkPlugIn.class.getName());



    /**
     * the base keep the description available.
     * To be sure this is correclty initialised, the factoryPlugIn, who create object, call the
     * initialise after
     */
    private MilkPlugInDescription description;

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
    }

    private TYPE_PLUGIN typePlugIn;

    /**
     * then only the factory can create an object
     * Default Constructor.
     */
    protected MilkPlugIn(TYPE_PLUGIN typePlugIn) {
        this.typePlugIn = typePlugIn;
    }

    public MilkPlugInDescription getDescription() {
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

    /**
     * check the PLUG IN environment, not the MilkJob environment
     * @param jobExecution 
     * 
     * @return
     */
    public abstract List<BEvent> checkPluginEnvironment(MilkJobExecution jobExecution);

    /**
     * check the JOB environnement, with the job parameters
     * 
     * @param jobExecution
     * @return
     */
    public abstract List<BEvent> checkJobEnvironment(MilkJobExecution jobExecution);

    /**
     * Initialise. This method is call by the Factory, after the object is created.
     * 
     * @param tenantId
     * @return
     */
    protected List<BEvent> initialize(Long tenantId) {
        List<BEvent> listEvents = new ArrayList<>();

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
     * PROCESSNAME : user will have a autocomplete list to select a process + version in existing process. Use MilkPlugInToolbox.completeListProcess() to
     * collect the SearchOptionBuilder()
     * ARRAYPROCESSNAME : same, but with an array of process. Use completeListProcess() to collect the SearchOptionBuilder()
     * ARRAY : an array on String
     * ARRAYMAP : then the arrayMapDescription has to be given
     * DELAY : format is <SCOPE>:VALUE. SCOPE are YEAR, MONTH, WEEK, DAY, HOUS, MN. Example: DAY:32. use MilkPlugInToolbox.getTimeFromDelay() to get the
     * TimeStamp value
     * FILEREAD : the plug in access this parameters in READ ONLY. Administrator is suppose to give
     * the file then.
     * FILEWRITE: the plug in write the file. Administrator can download it only
     * FILEREADWRITE : plug in and administrator can read and write the file
     */
    public enum TypeParameter {
        USERNAME, PROCESSNAME, ARRAYPROCESSNAME, STRING, TEXT, DELAY, JSON, LONG, OBJECT, ARRAY, BOOLEAN, ARRAYMAP, BUTTONARGS, FILEREAD, FILEWRITE, FILEREADWRITE, LISTVALUES, SEPARATOR
    }

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
            Map<String, Object> map = new HashMap<>();

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
        public boolean isMandatory= false;
        public String visibleCondition=null;
        
        // a button have args : give it the number of args 
        public String buttonDescription;
        public List<String> argsName;
        public List<String> argsValue;

        public String[] listValues = null;
        // a FILEWRITE, FILEREAD, FILEREADWRITE has a content type and a file name
        public String fileName;
        public String contentType;

        public List<ColDefinition> arrayMapDescription;

        public static PlugInParameter createInstance(String name, String label, TypeParameter typeParameter, Object defaultValue, String explanation) {
           return createInstance(name, label, typeParameter, defaultValue, explanation, false);
        }
        /**
         * 
         * @param name
         * @param label
         * @param typeParameter
         * @param defaultValue
         * @param explanation
         * @param isMandatory
         * @param visibleCondition use a condition. To check with the value of other parameters, use "milkJob.parametersvalue[ <nameOfParameter> ]". Example "milkJob.parametersvalue[ 'operation' ] != 'GetFromList'"
         * @return
         */
        public static PlugInParameter createInstance(String name, String label, TypeParameter typeParameter, Object defaultValue, String explanation, boolean isMandatory, String visibleCondition) {
            PlugInParameter plugInParameter = new PlugInParameter();
            plugInParameter.name = name;
            plugInParameter.label = label;
            plugInParameter.isMandatory = isMandatory;
            plugInParameter.visibleCondition =  visibleCondition;
            plugInParameter.typeParameter = typeParameter;
            plugInParameter.defaultValue = defaultValue;
            plugInParameter.explanation = explanation;
            // set an empty value then
            if (typeParameter == TypeParameter.ARRAY && defaultValue == null)
                plugInParameter.defaultValue = new ArrayList<Object>();

            return plugInParameter;
        }
        public static PlugInParameter createInstance(String name, String label, TypeParameter typeParameter, Object defaultValue, String explanation, boolean isMandatory) {
            
            return createInstance(name, label, typeParameter,defaultValue,explanation,isMandatory,null);
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
            return createInstanceFile(name, label, typeParameter, defaultValue, explanation, fileName,contentType, false, null);
        }
        public static PlugInParameter createInstanceFile(String name, String label, TypeParameter typeParameter, Object defaultValue, String explanation, String fileName, String contentType, boolean isMandatory,  String visibleCondition) {
            PlugInParameter plugInParameter = new PlugInParameter();
            plugInParameter.name = name;
            plugInParameter.label = label;
            plugInParameter.typeParameter = typeParameter;
            plugInParameter.defaultValue = defaultValue;
            plugInParameter.explanation = explanation;
            plugInParameter.fileName = fileName;
            plugInParameter.contentType = contentType;
            plugInParameter.isMandatory = isMandatory;
            plugInParameter.visibleCondition= visibleCondition;

            // set an empty value then
            if (typeParameter == TypeParameter.ARRAY && defaultValue == null)
                plugInParameter.defaultValue = new ArrayList<Object>();

            return plugInParameter;
        }

        public static PlugInParameter createInstanceListValues(String name, String label, String[] listValues, Object defaultValue, String explanation) {
            PlugInParameter plugInParameter = new PlugInParameter();
            plugInParameter.name = name;
            plugInParameter.label = label;
            plugInParameter.typeParameter = TypeParameter.LISTVALUES;
            plugInParameter.listValues = listValues;
            plugInParameter.defaultValue = defaultValue;
            plugInParameter.explanation = explanation;
            // set an empty value then
            if (plugInParameter.typeParameter == TypeParameter.ARRAY && defaultValue == null)
                plugInParameter.defaultValue = new ArrayList<Object>();

            return plugInParameter;
        }

        /**
         * here the JSON returned to the HTML to build the parameters display
         * 
         * @return
         */
        public Map<String, Object> getMap() {
            Map<String, Object> map = new HashMap<>();
            map.put(MilkConstantJson.CSTJSON_PLUGIN_PARAMETERNAME, name);
            map.put(MilkConstantJson.CSTJSON_PLUGIN_PARAMETERLABEL, label);
            map.put(MilkConstantJson.CSTJSON_PLUGIN_PARAMETERMANDATORY, isMandatory);
            map.put(MilkConstantJson.CSTJSON_PLUGIN_PARAMETERVISIBLECONDITION, visibleCondition);
            
            map.put("type", typeParameter.toString());
            if (arrayMapDescription != null) {
                List<Map<String, Object>> listDescription = new ArrayList<>();
                for (ColDefinition col : arrayMapDescription) {
                    listDescription.add(col.getMap());
                }
                map.put("arraymapDefinition", listDescription);
            }
            if (MilkPlugIn.TypeParameter.LISTVALUES.equals(typeParameter)) {
                map.put("listValues", Arrays.asList(listValues));

            }

            if (MilkPlugIn.TypeParameter.BUTTONARGS.equals(typeParameter)) {
                List<Map<String, Object>> listArgs = new ArrayList<>();
                if (argsName != null)
                    for (int i = 0; i < argsName.size(); i++) {
                        Map<String, Object> mapArgs = new HashMap<>();
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

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Mesure */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    public static @Data class PlugInMesure {
        public String name;
        public String label;
        public String explanation;
        
        public static PlugInMesure createInstance(String name, String label, String explanation ) {
            PlugInMesure plugInMesure = new PlugInMesure();
            plugInMesure.name = name;
            plugInMesure.label = label;
            plugInMesure.explanation = explanation;
            return plugInMesure;
        }
        public Map<String,Object> getMap() {
            Map<String,Object> result = new HashMap<>();
            result.put( MilkConstantJson.CSTJSON_MEASUREMENT_PLUGIN_NAME, name);
            result.put( MilkConstantJson.CSTJSON_MEASUREMENT_PLUGIN_LABEL, label);
            result.put( MilkConstantJson.CSTJSON_MEASUREMENT_PLUGIN_EXPLANATION, explanation);
            return result;
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

    private int nbDefaultSavedExecution = 10;
    private int nbDefaultHistoryMesures =  200;
            
    public int getDefaultNbSavedExecution() {
        return nbDefaultSavedExecution;
    }
    public void setDefaultNbSavedExecution( int nbSavedExecution ) {
        this.nbDefaultSavedExecution = nbSavedExecution;
    }
    public int getDefaultNbHistoryMesures() {
        return nbDefaultHistoryMesures;
    }
    public void setDefaultNbHitoryMesures( int nbDefaultHistoryMesure ) {
        this.nbDefaultHistoryMesures = nbDefaultHistoryMesure;
    }
    
    
    /**
     * describe the plug in
     * 
     * @return
     */
    public Map<String, Object> getMap() {
        Map<String, Object> result = new HashMap<>();
        result.put("name", description == null ? null : description.name);
        result.put( MilkConstantJson.CSTJSON_PLUGIN_DISPLAYNAME, description == null ? null : description.label);
        result.put( MilkConstantJson.CSTJSON_PLUGIN_EXPLANATION, description == null ? null : description.getExplanation());
        result.put( MilkConstantJson.CSTJSON_PLUGIN_EMBEDED, typePlugIn == TYPE_PLUGIN.EMBEDED);
        result.put( MilkConstantJson.CSTJSON_PLUGIN_LOCAL, typePlugIn == TYPE_PLUGIN.LOCAL);
        result.put( MilkConstantJson.CSTJSON_PLUGIN_CMD, typePlugIn == TYPE_PLUGIN.COMMAND);
        result.put( MilkConstantJson.CSTJSON_PLUGIN_CATEGORY, description == null ? null : description.category.toString());
        result.put( MilkConstantJson.CSTJSON_PLUGIN_TYPE, typePlugIn.toString());

        return result;

    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Notification                                                                     */
    /*                                                                                  */
    /* some plugIn may need to adjust external component. Then, at each change, a       */
    /* notification is sent                                                             */
    /*                                                                                  */
    /* ******************************************************************************** */
    public List<BEvent> notifyRegisterAJob( MilkJob milkJob,MilkJobExecution jobExecution ) {
        return new ArrayList<> ();
    }
    public List<BEvent> notifyUnregisterAJob( MilkJob milkJob,MilkJobExecution jobExecution ) {
        return new ArrayList<> ();
    }
    public List<BEvent> notifyUpdateParameters( MilkJob milkJob,MilkJobExecution jobExecution ) {
        return new ArrayList<> ();
    }

    public List<BEvent> notifyActivateAJob( MilkJob milkJob,MilkJobExecution jobExecution ) {
        return new ArrayList<> ();
    }
    public List<BEvent> notifyUnactivateAJob( MilkJob milkJob,MilkJobExecution jobExecution ) {
        return new ArrayList<> ();
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
    public abstract MilkPlugInDescription getDefinitionDescription();

    public abstract MilkJobOutput execute(MilkJobExecution input);

    /**
     * this method can be override by the plug in if it create a button "BUTTONPARAMETERS"
     * 
     * @param parameters
     * @return
     */
    public List<BEvent> buttonParameters(String buttonName, MilkJobExecution input, Map<String, Object> argsParameters, APIAccessor apiAccessor) {
        return new ArrayList<>();
    }

}
