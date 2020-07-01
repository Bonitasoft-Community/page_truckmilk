package org.bonitasoft.truckmilk.engine;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.properties.BonitaProperties;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJob.MapContentParameter;
import org.bonitasoft.truckmilk.schedule.MilkScheduleQuartz;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerFactory;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt;
import org.bonitasoft.truckmilk.toolbox.MilkLog;
import org.json.simple.JSONValue;

/**
 * this class manage the serialization of the TOur in Bonita Properties
 * 
 * @author Firstname Lastname
 */
public class MilkSerializeProperties {

    private static MilkLog logger = MilkLog.getLogger(MilkSerializeProperties.class.getName());

    /*
     * we can use multiple bonitaProperties at a time (multithread), assuming the store() is a transaction in the database and protect multiple write
     */
    private BonitaProperties bonitaProperties;

    public final static String BONITAPROPERTIESNAME = "MilkTour";
    
    
    private final static String BONITAPROPERTIESDOMAIN = "tour";
    // private final static String BonitaPropertiesMainInfo = "maininfo";
    private MilkJobFactory milkJobFactory;

    private static BEvent eventBadTourJsonFormat = new BEvent(MilkSerializeProperties.class.getName(), 1,
            Level.APPLICATIONERROR,
            "Bad tour format", "A Milk tour can't be read due to a Json format", "This tour is lost", "Reconfigure it");

    public static class SerializeOperation {

        public List<BEvent> listEvents = new ArrayList<>();
        public MilkSchedulerInt scheduler;
    }

    public MilkSerializeProperties(MilkJobFactory milkPlugInTourFactory) {
        this.milkJobFactory = milkPlugInTourFactory;
        bonitaProperties = new BonitaProperties(BONITAPROPERTIESNAME, milkPlugInTourFactory.getMilkJobContext().getTenantId());
        bonitaProperties.setPolicyBigStringToStream(true);
        bonitaProperties.setLogLevel(java.util.logging.Level.FINE);

    }

    public MilkSerializeProperties(long tenantId) {
        bonitaProperties = new BonitaProperties(BONITAPROPERTIESNAME, tenantId);
        bonitaProperties.setPolicyBigStringToStream(true);
        bonitaProperties.setLogLevel(java.util.logging.Level.FINE);
    }

    public List<BEvent> checkAndUpdateEnvironment() {
        List<BEvent> listEvents = new ArrayList<>();
        bonitaProperties.setCheckDatabase(true);
        bonitaProperties.setLogCheckDatabaseAtFirstAccess(true);

        listEvents.addAll(bonitaProperties.loaddomainName(BONITAPROPERTIESDOMAIN));
        return listEvents;
    }

    /**
     * get a PlugInTour from the Bonitaproperties. The IdTour has to be detected first
     * 
     * @param idTour
     * @param bonitaProperties
     * @param milkJobFactory
     * @return
     */
    private MilkJob getInstanceFromBonitaProperties(Long jobId /*, SaveJobParameters loadParameters */) {
        String jobSt = (String) bonitaProperties.get(jobId.toString());
        logger.fine(".getInstanceFromBonitaProperties begin Read JobId[" + jobId + "] jsonst=" + jobSt);
        
        
        MilkJob milkJob = MilkJob.getInstanceFromMap( getMapFromJsonSt( jobSt ), milkJobFactory);
        if (milkJob == null) {
            logger.info(".getInstanceFromBonitaProperties end because milJob=null ?");
            return null; // not a normal way
        }

        //-----------------------  read the another information

        String jsonStTrackExecution = (String) bonitaProperties.get(jobId.toString() + CSTPREFIXPROPERTIES_TRACKEXECUTION);
        if (jsonStTrackExecution != null)
            milkJob.readTrackExecutionFromMap(getMapFromJsonSt( jsonStTrackExecution) );

        
        
        String jsonStMesures = (String) bonitaProperties.get(jobId.toString() + CSTPREFIXPROPERTIES_MEASUREMENT);
        if (jsonStMesures != null)
            milkJob.readMeasurementValuesFromList(getListFromJsonSt( jsonStMesures) );

        // askstop has its own variable, so read it after the track execution
        Object askStopObj = bonitaProperties.get(jobId.toString() + CSTPREFIXPROPERTIES_ASKSTOP);
        if (askStopObj != null)
            milkJob.setAskForStop( Boolean.valueOf(askStopObj.toString()));

        String jsonStSavedExecution = (String) bonitaProperties.get(jobId.toString() + CSTPREFIXPROPERTIES_SAVEDEXECUTION);
        if (jsonStSavedExecution != null)
            milkJob.readSavedExecutionFromList( getListFromJsonSt( jsonStSavedExecution) );

        // load File Parameters
        MilkPlugInDescription plugInDescription = milkJob.getPlugIn().getDescription();
        for (PlugInParameter parameter : plugInDescription.getInputParameters()) {
            if (parameter.typeParameter == TypeParameter.FILEWRITE || parameter.typeParameter == TypeParameter.FILEREADWRITE || parameter.typeParameter == TypeParameter.FILEREAD) {
                milkJob.setParameterStream(parameter, bonitaProperties.getPropertyStream(milkJob.getId() + CSTPREFIXPROPERTIES_STREAMVALUE + parameter.name));
            }
        }
        logger.fine(".getInstanceFromBonitaProperties end");

        return milkJob;
    }

    public List<BEvent> dbLoadAllJobs() {
        return dbLoadAllJobsAndPurge(false);
    }

    public List<BEvent> dbLoadAllJobsAndPurge(boolean purge) {
        List<BEvent> listEvents = new ArrayList<>();
        bonitaProperties.setCheckDatabase(false);
        bonitaProperties.setPolicyBigStringToStream(true);
        logger.fine(".dbLoadAllJobsAndPurge-begin");

        /** soon : name is NULL to load all tour */
        listEvents.addAll(bonitaProperties.loaddomainName(BONITAPROPERTIESDOMAIN));
        // bonitaProperties.traceInLog();
        List<String> listToursToDelete = new ArrayList<>();
        Enumeration<?> enumKey = bonitaProperties.propertyNames();
        while (enumKey.hasMoreElements()) {
            String idTourSt = (String) enumKey.nextElement();
            // String plugInTourSt = (String) bonitaProperties.get(idTour);
            Long idTour = null;
            try {
                if (idTourSt.endsWith(CSTPREFIXPROPERTIES_ASKSTOP) ||   idTourSt.endsWith(CSTPREFIXPROPERTIES_TRACKEXECUTION) || idTourSt.endsWith(CSTPREFIXPROPERTIES_SAVEDEXECUTION) || idTourSt.endsWith(CSTPREFIXPROPERTIES_MEASUREMENT))
                    continue;
                idTour = Long.valueOf(idTourSt);
            } catch (Exception e) {
            }
            if (idTour == null)
                continue;
            // we may have different key now
            MilkJob plugInTour = getInstanceFromBonitaProperties(idTour);

            if (plugInTour != null) {
                // register in the factory now
                milkJobFactory.putJob(plugInTour);
            } else {
                listToursToDelete.add(idTourSt);
                listEvents.add(new BEvent(eventBadTourJsonFormat, "Id[" + idTour + "]"));
            }
        }

        if (purge) {
            Set<String> keysToDelete = new HashSet<String>();
            keysToDelete.addAll(listToursToDelete);
            if (listToursToDelete.size() > 0) {
                for (String idTour : listToursToDelete)
                    bonitaProperties.remove(idTour);
            }

            // check the properties name
            Enumeration<?> enumStream = bonitaProperties.propertyStream();
            List<String> listStreamToDelete = new ArrayList<>();

            while (enumStream.hasMoreElements()) {
                // is the Id still exist?
                String idStream = (String) enumStream.nextElement();
                // expect <idTour>_<parameterName>
                if (idStream.indexOf("_") == -1)
                    continue;
                String idTour = idStream.substring(0, idStream.indexOf("_"));
                try {
                    Long idTourL = Long.valueOf(idTour);
                    if (!milkJobFactory.existJob(idTourL)) {
                        listStreamToDelete.add(idStream);
                    }
                } catch (Exception e) {
                } ; // idTour is not a long... strange, then do nothing
            }
            if (! listStreamToDelete.isEmpty()) {
                for (String idStream : listStreamToDelete) {
                    bonitaProperties.removeStream(idStream);
                }
            }
            keysToDelete.addAll(listToursToDelete);
            if (! keysToDelete.isEmpty()) {
                // do the deletion now
                logger.info(".dbLoadAllJobsAndPurge Delete Key and Stream [" + keysToDelete + "]");
                bonitaProperties.storeCollectionKeys(keysToDelete);
            }
        }
        logger.fine(".dbLoadAllJobsAndPurge-end");

        return listEvents;
    }

    /**
     * @param idTour
     * @return
     */
    public MilkJob dbLoadJob(Long jobId) {
        bonitaProperties.setCheckDatabase(false);

        logger.fine(".dbLoadJob - begin");

        /** soon : name is NULL to load all tour */
        bonitaProperties.loaddomainName(BONITAPROPERTIESDOMAIN);

        MilkJob milkJob = getInstanceFromBonitaProperties(jobId);
        // register in the factory now
        milkJobFactory.putJob(milkJob);

        logger.fine(".dbLoadJob - end");

        return milkJob;
    }

    public List<BEvent> dbSaveAllJobs(MilkJobContext milkJobContext) {
        List<BEvent> listEvents = new ArrayList<>();
        bonitaProperties.setCheckDatabase(false);
        logger.info(".dbSaveAllJobs-begin ***");

        listEvents.addAll(bonitaProperties.loaddomainName(BONITAPROPERTIESDOMAIN));
        for (MilkJob milkJob : milkJobFactory.getMapJobsId().values()) {
            dbSaveMilkJob(milkJob, milkJobContext, SerializationJobParameters.getInstanceAllInformations());
        }
        logger.info(".dbSaveAllJobs-end");

        return listEvents;
    }

    /**
     * Do to asynchronous, when a user ask to stop, we must save it. In the same time, if the job report a parameters update, then we have to save it, but do
     * not override the previous update.
     */
    public static class SerializationJobParameters {

        boolean fileRead = false;
        boolean fileWrite = false;
        boolean askStop = false;
        boolean trackExecution = false;
        boolean savedExecution = false;
        boolean saveBase = false;
        boolean mesures=false;

        public static SerializationJobParameters getInstanceTrackExecution() {
            SerializationJobParameters saveParameters = new SerializationJobParameters();
            saveParameters.trackExecution = true;
            return saveParameters;
        }

        /**
         * sall all main information. End of an execution for example
         * 
         * @return
         */
        public static SerializationJobParameters getInstanceAllInformations() {
            SerializationJobParameters saveParameters = new SerializationJobParameters();
            saveParameters.trackExecution = true;
            saveParameters.askStop = true;
            saveParameters.saveBase = true;
            return saveParameters;
        }

        public static SerializationJobParameters getInstanceEverything() {
            SerializationJobParameters saveParameters = new SerializationJobParameters();
            saveParameters.trackExecution = true;
            saveParameters.askStop = true;
            saveParameters.saveBase = true;
            saveParameters.fileRead = true;
            saveParameters.fileWrite = true;

            return saveParameters;
        }

        public static SerializationJobParameters getInstanceStartExecutionJob() {
            SerializationJobParameters saveParameters = new SerializationJobParameters();
            saveParameters.trackExecution = true;
            saveParameters.savedExecution = true; // we want to save the start execution
            saveParameters.askStop = true;
            saveParameters.saveBase = false;
            saveParameters.fileRead = false;
            saveParameters.fileWrite = false;

            return saveParameters;
        }

        public static SerializationJobParameters getInstanceEndExecutionJob() {
            SerializationJobParameters saveParameters = new SerializationJobParameters();
            saveParameters.trackExecution = true;
            saveParameters.savedExecution = true;
            saveParameters.askStop = true;
            saveParameters.saveBase = false;
            saveParameters.fileRead = false;
            saveParameters.fileWrite = true;
            saveParameters.mesures = true;

            return saveParameters;
        }

        /**
         * Save the based information. Do not save the execution / askstop information
         * 
         * @return
         */
        public static SerializationJobParameters getBaseInformations() {
            SerializationJobParameters saveParameters = new SerializationJobParameters();
            saveParameters.saveBase = true;
            return saveParameters;
        }

        public static SerializationJobParameters getAskStop() {
            SerializationJobParameters saveParameters = new SerializationJobParameters();
            saveParameters.askStop = true;
            return saveParameters;
        }

    }

    /**
     * top let the properties purge only what is needed, each item must start by an unique name
     */
    private final static String CSTPREFIXPROPERTIES_BASE = "";
    private final static String CSTPREFIXPROPERTIES_STREAMVALUE = "_s_";

    private final static String CSTPREFIXPROPERTIES_ASKSTOP = "_askStop";
    private final static String CSTPREFIXPROPERTIES_TRACKEXECUTION = "_trackExec";
    private final static String CSTPREFIXPROPERTIES_SAVEDEXECUTION = "_savedExecution";
    private final static String CSTPREFIXPROPERTIES_MEASUREMENT = "_mesures";

    //  public static String prefixPropertiesTrackStream = "_stream";

    public List<BEvent> dbSaveMilkJob(MilkJob milkJob, MilkJobContext milkJobContext, SerializationJobParameters saveParameters) {
        List<BEvent> listEvents = new ArrayList<>();

        bonitaProperties.setCheckDatabase(false);
        listEvents.addAll(bonitaProperties.loaddomainName(BONITAPROPERTIESDOMAIN));

        Set<String> listKeys = new HashSet<>();
        // save the job
        if (saveParameters.saveBase) {
            MapContentParameter mapContentParameter = new MapContentParameter();
            mapContentParameter.askStop = false;
            mapContentParameter.savedExecution = false;
            mapContentParameter.trackExecution = false;

            bonitaProperties.put(String.valueOf(milkJob.getId()) + CSTPREFIXPROPERTIES_BASE, getJsonSt( milkJob.getMap(mapContentParameter, milkJobContext)));
            listKeys.add(milkJob.getId() + CSTPREFIXPROPERTIES_BASE);
        }
        if (saveParameters.askStop) {
            bonitaProperties.put(String.valueOf(milkJob.getId()) + CSTPREFIXPROPERTIES_ASKSTOP, milkJob.getAskForStop());
            listKeys.add(milkJob.getId() + CSTPREFIXPROPERTIES_ASKSTOP);
        }
        if (saveParameters.savedExecution) {
            bonitaProperties.put(String.valueOf(milkJob.getId()) + CSTPREFIXPROPERTIES_SAVEDEXECUTION, getJsonSt( milkJob.getMapListSavedExecution( false, false )));
            listKeys.add(milkJob.getId() + CSTPREFIXPROPERTIES_SAVEDEXECUTION);
        }
        if (saveParameters.trackExecution) {
            bonitaProperties.put(String.valueOf(milkJob.getId()) + CSTPREFIXPROPERTIES_TRACKEXECUTION, getJsonSt( milkJob.getTrackExecutionMap()) );
            listKeys.add(milkJob.getId() + CSTPREFIXPROPERTIES_TRACKEXECUTION);
        }
        
        if (saveParameters.mesures) {
            bonitaProperties.put(String.valueOf(milkJob.getId()) + CSTPREFIXPROPERTIES_MEASUREMENT, getJsonSt( milkJob.getListMeasurementValues(false)) );
            listKeys.add(milkJob.getId() + CSTPREFIXPROPERTIES_MEASUREMENT);
        }
        
        // save all
        MilkPlugInDescription plugInDescription = milkJob.getPlugIn().getDescription();
        for (PlugInParameter parameter : plugInDescription.getInputParameters()) {
            boolean saveFile = false;
            if (saveParameters.fileWrite && (parameter.typeParameter == TypeParameter.FILEWRITE || parameter.typeParameter == TypeParameter.FILEREADWRITE))
                saveFile = true;
            if (saveParameters.fileRead && (parameter.typeParameter == TypeParameter.FILEREADWRITE || parameter.typeParameter == TypeParameter.FILEREAD))
                saveFile = true;
            if (saveFile) {
                bonitaProperties.setPropertyStream(milkJob.getId() + CSTPREFIXPROPERTIES_STREAMVALUE + parameter.name, milkJob.getParameterStream(parameter));
                listKeys.add(milkJob.getId() + CSTPREFIXPROPERTIES_STREAMVALUE + parameter.name);
            }
        }
        logger.fine(".dbSaveJob-begin keys=" + listKeys.toString());

        listEvents.addAll(bonitaProperties.storeCollectionKeys(listKeys));
        logger.fine(".dbSaveJob-end");

        return listEvents;
    }

    /**
     * Json a map
     * @param value : maybe a Map, a List...
     * @return
     */
    public String getJsonSt(Object value ) {
        return JSONValue.toJSONString( value );
    }
    
    @SuppressWarnings("unchecked")
    public Map<String, Object> getMapFromJsonSt(String jsonSt ) {
        try {
            return (Map<String, Object>) JSONValue.parse(jsonSt);
        }
        catch(Exception e) 
        {
            return null;
        }
    }
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> getListFromJsonSt(String jsonSt ) {
        try {
            return (List<Map<String, Object>>) JSONValue.parse(jsonSt);
        }
        catch(Exception e) 
        {
            return null;
        }
    }
    
 
    /**
     * reload the tour from the properties
     * 
     * @param bonitaProperties
     * @param milkCmdControl
     *        public void dbReloadLoadPlugInTour(BonitaProperties bonitaProperties, MilkPlugInTourFactory milkPlugInTourFactory) {
     *        String plugInTourSt = (String) bonitaProperties.get(String.valueOf(id));
     *        MilkPlugInTour plugInTourProperties = MilkPlugInTour.getInstanceFromJson(plugInTourSt, milkPlugInTourFactory);
     *        copyFrom(plugInTourProperties, milkPlugInTourFactory);
     *        }
     */

    public List<BEvent> dbRemoveMilkJob(MilkJob milkJob) {
        List<BEvent> listEvents = new ArrayList<>();
        bonitaProperties.setCheckDatabase(false);

        listEvents.addAll(bonitaProperties.loaddomainName(BONITAPROPERTIESDOMAIN));
        logger.info(".dbRemovePlugInTour-begin idJob=" + milkJob.getId());

        // remove(pluginTour.getId() does not work, so let's compare the key
        Set<String> listKeys = new HashSet<>();
        listKeys.add(milkJob.getId() + CSTPREFIXPROPERTIES_BASE);
        listKeys.add(milkJob.getId() + CSTPREFIXPROPERTIES_ASKSTOP);
        listKeys.add(milkJob.getId() + CSTPREFIXPROPERTIES_TRACKEXECUTION);

        for (String key : listKeys)
            bonitaProperties.remove(key);

        listEvents.addAll(bonitaProperties.storeCollectionKeys(listKeys));
        logger.info(".dbRemovePlugInTour-end");

        return listEvents;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Save Scheduler information */
    /*                                                                                  */
    /* ******************************************************************************** */

    private final static String BONITAPROPERTIES_DOMAINSCHEDULER = "scheduler";
    private final static String BONITAPROPERTIES_SCHEDULER_SCHEDULERTYPE = "schedulertype";
    private final static String BONITAPROPERTIES_SCHEDULER_REPORTLOGHEART = "reportheart";
    

    public SerializeOperation getCurrentScheduler(MilkSchedulerFactory schedulerFactory) {
        SerializeOperation serializeOperation = new SerializeOperation();

        bonitaProperties.setCheckDatabase(false); // we check only when we deploy a new command
        serializeOperation.listEvents.addAll(bonitaProperties.loaddomainName(BONITAPROPERTIES_DOMAINSCHEDULER));
        String valueSt = (String) bonitaProperties.get(BONITAPROPERTIES_SCHEDULER_SCHEDULERTYPE);
        serializeOperation.scheduler = schedulerFactory.getFromType(valueSt, true);

        Object reportHeart= bonitaProperties.get(BONITAPROPERTIES_SCHEDULER_REPORTLOGHEART);
        serializeOperation.scheduler.setLogHeartBeat( reportHeart==null ? false : "true".equals( reportHeart.toString() ));
        
        return serializeOperation;
    }

    public SerializeOperation saveCurrentScheduler(MilkSchedulerInt currentScheduler) {
        logger.fine(".saveCurrentScheduler-begin");
        Set<String> listKeys = new HashSet<>();

        SerializeOperation serializeOperation = new SerializeOperation();
        bonitaProperties.setCheckDatabase(false); // we check only when we deploy a new command
        serializeOperation.listEvents.addAll(bonitaProperties.loaddomainName(BONITAPROPERTIES_DOMAINSCHEDULER));
        
        bonitaProperties.put(BONITAPROPERTIES_SCHEDULER_SCHEDULERTYPE, currentScheduler.getType().toString());
        listKeys.add(BONITAPROPERTIES_SCHEDULER_SCHEDULERTYPE);
       
        MilkReportEngine reportEngine = MilkReportEngine.getInstance();
        bonitaProperties.put(BONITAPROPERTIES_SCHEDULER_REPORTLOGHEART, currentScheduler.isLogHeartBeat()? "true" : "false");
        listKeys.add(BONITAPROPERTIES_SCHEDULER_REPORTLOGHEART);

        serializeOperation.listEvents.addAll(bonitaProperties.storeCollectionKeys(listKeys));

        logger.fine(".saveCurrentScheduler-end");

        return serializeOperation;
    }

  

}
