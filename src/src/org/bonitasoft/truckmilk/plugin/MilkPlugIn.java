package org.bonitasoft.truckmilk.plugin;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.api.TenantAPIAccessor;
import org.bonitasoft.engine.command.SCommandExecutionException;
import org.bonitasoft.engine.command.SCommandParameterizationException;
import org.bonitasoft.engine.command.TenantCommand;
import org.bonitasoft.engine.connector.ConnectorAPIAccessorImpl;
import org.bonitasoft.engine.service.ServiceAccessor;
import org.bonitasoft.engine.service.TenantServiceAccessor;
import org.bonitasoft.log.event.BEvent;
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
 * 
 * Register the embeded plug in in MilkCmdControl.detectListPlugIn()
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

public abstract class MilkPlugIn  {

 
    /* ******************************************************************************** */
    /*                                                                                  */
    /* Definition */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    
    public MilkPlugIn()
    {        
    }
   
    /**
     * an embeded plugin means the plugin is deliver inside the Miltour library. Then, the MilkCmdControl does not need to use the Command communication
     * 
     * @return
     */
    public boolean isEmbeded() {
        return false;
    };
    
    public List<BEvent> initialize(Long tenantId)
    {
        List<BEvent> listEvents = new ArrayList<BEvent>();
        
        // if isEmbeded, just get the description and save it localy
        if (isEmbeded())
            description = getDescription();
        else
        {
            // collect the description by calling the command
            
            // and then get it localy
        }
        return listEvents;
    }
    
    public enum TypeParameter { STRING, TEXT, LONG, OBJECT, ARRAY, BOOLEAN };
    public static class PlugInParameter
    {
        public String name;
        public Object defaultValue;
        public String explanation;
        public TypeParameter typeParameter;
        public static PlugInParameter createInstance(String name, TypeParameter typeParameter, Object defaultValue, String explanation)
        {
            PlugInParameter plugInParameter = new PlugInParameter();
            plugInParameter.name = name;
            plugInParameter.typeParameter = typeParameter;
            plugInParameter.defaultValue = defaultValue;
            plugInParameter.explanation = explanation;
            // set an empty value then
            if (typeParameter==TypeParameter.ARRAY && defaultValue==null )
                plugInParameter.defaultValue = new ArrayList();

            return plugInParameter;
        }
        public Map<String,Object> getMap()
        {
            Map<String,Object> map = new HashMap<String,Object>();
            map.put("name", name);
            map.put("type", typeParameter.toString());
            map.put("explanation", explanation);
            map.put("defaultValue", defaultValue);
            return map;
        }
    }
    public static class PlugInDescription
    {
        public String name;
        public String version;
        public String displayName;
        public String description;
        public List<PlugInParameter> inputParameters = new ArrayList<PlugInParameter>();
        public String cronSt = "0 0/10 * 1/1 * ? *"; // every 10 mn
        
        public void addParameter( PlugInParameter parameter)
        {
            inputParameters.add( parameter );
        }
        /**
         * Expected format : "{\"delayinmn\":10,\"maxtentative\":12,\"processfilter\":[{\"name\":\"expens*\",\"version\":null}]}")
         * @param jsonSt
         */
        public void addParameterFromMapJson(String jsonSt )
        {
            Map<String, Object> mapJson= (Map<String, Object>) JSONValue.parse(jsonSt);
            for (String key : mapJson.keySet())
                addParameter( PlugInParameter.createInstance( key, TypeParameter.STRING, mapJson.get(key), null));
        }
        public Map<String,Object> getParametersMap()
        {
            Map<String,Object> map = new HashMap<String,Object>();
            for( PlugInParameter plugInParameter : inputParameters)
            {
                map.put( plugInParameter.name, plugInParameter.defaultValue);
            }
            return map;
        }
        
    }
    private PlugInDescription description; 
   
    public final PlugInDescription getLocalDescription()
    {
        return description;
    }
    /**
     * get the name, should be unique in all plugin
     * @return
     */
    public String getName()
    {
        return (description==null ? this.getClass().getName() : description.name);    
    }
    /**
     * describe the plug in
     * 
     * @return
     */
    public Map<String, Object> getMap() {
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("name", description==null ? null : description.name);
        result.put("displayname", description==null ? null : description.displayName);
        result.put("description", description==null ? null : description.description);
        result.put("embeded", isEmbeded());

        return result;

    }
    /* ******************************************************************************** */
    /*                                                                                  */
    /* Execution                                                                        */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    public static class PlugTourInput {

        public String tourName;
        public Map<String, Object> tourParameters;
        
        private MilkPlugInTour milkPlugInTour;
        public PlugTourInput( MilkPlugInTour milkPlugInTour)
        {
            this.milkPlugInTour = milkPlugInTour;
        }
        
        public PlugTourOutput getPlugTourOutput()
        {
            return new PlugTourOutput(milkPlugInTour);
        }
        
        /** Boolean
         * 
         * @param parameter
         * @return
         */
        public Boolean getInputBooleanParameter( PlugInParameter parameter)
        {
            return getInputBooleanParameter(parameter.name, (Boolean)parameter.defaultValue);
        }
        public Boolean getInputBooleanParameter(String name, Boolean defaultValue )
        {
            try
            {
                return Boolean.valueOf(tourParameters.get( name ).toString());
            }
            catch(Exception e)
            {
                return defaultValue;
            }
        }

        /**
         * Long
         * @param parameter
         * @return
         */
        public Long getInputLongParameter( PlugInParameter parameter)
        {
            return getInputLongParameter(parameter.name, (Long)parameter.defaultValue);
        }
        
        public Long getInputLongParameter(String name, Long defaultValue )
        {
            try
            {
                return Long.valueOf(tourParameters.get( name ).toString());
            }
            catch(Exception e)
            {
                return defaultValue;
            }
        }
        public String getInputStringParameter( PlugInParameter parameter)
        {
            return getInputStringParameter(parameter.name, (String)parameter.defaultValue);
        }
        public String getInputStringParameter(String name, String defaultValue )
        {
            try
            {
                return (String)tourParameters.get( name );
            }
            catch(Exception e)
            {
                return defaultValue;
            }
        }
        public List getInputListParameter( PlugInParameter parameter)
        {
            return getInputListParameter(parameter.name, (List)parameter.defaultValue);
        }

        public List getInputListParameter(String name, List defaultValue )
        {
            try
            {
                return (List)tourParameters.get( name );
            }
            catch(Exception e)
            {
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
         * 
         */
        public List<BEvent> listEvents =null;

        // the listEvents is saved in the St
        public String listEventsSt="";
        /**
         * give a simple status execution.
         */
        public ExecutionStatus executionStatus = ExecutionStatus.NOEXECUTION;

        /** save the time need to the execution */
        public Long executionTimeInMs;
        
        public MilkPlugInTour plugInTour;
        
        public String explanation;
        
        // if you have a PlugInTourInput, create the object from this
        public PlugTourOutput(MilkPlugInTour plugInTour)
        {
            this.plugInTour = plugInTour;
        }
        public void addEvent( BEvent event )
        {
            // new execution : create the list event to collect it
            if (listEvents==null)
                listEvents= new ArrayList<BEvent>();
            listEvents.add( event );
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
