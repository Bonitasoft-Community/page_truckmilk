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
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.job.MilkJob;
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
    BonitaProperties bonitaProperties;

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
        bonitaProperties = new BonitaProperties(BONITAPROPERTIESNAME, milkPlugInTourFactory.getTenantId());
        bonitaProperties.setLogLevel(java.util.logging.Level.FINE);

    }

    public MilkSerializeProperties(long tenantId) {
        bonitaProperties = new BonitaProperties(BONITAPROPERTIESNAME, tenantId);
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

        String jsonStTrackExecution = (String) bonitaProperties.get(jobId.toString() + prefixPropertiesTrackExecution);
        if (jsonStTrackExecution != null)
            milkJob.readTrackExecutionFromMap(getMapFromJsonSt( jsonStTrackExecution) );

        // askstop has its own variable, so read it after the track execution
        Object askStopObj = bonitaProperties.get(jobId.toString() + prefixPropertiesAskStop);
        if (askStopObj != null)
            milkJob.setAskForStop( Boolean.valueOf(askStopObj.toString()));

        String jsonStSavedExecution = (String) bonitaProperties.get(jobId.toString() + prefixPropertiesSavedExecution);
        if (jsonStSavedExecution != null)
            milkJob.readSavedExecutionFromList( getListFromJsonSt( jsonStSavedExecution) );

        // load File Parameters
        PlugInDescription plugInDescription = milkJob.getPlugIn().getDescription();
        for (PlugInParameter parameter : plugInDescription.inputParameters) {
            if (parameter.typeParameter == TypeParameter.FILEWRITE || parameter.typeParameter == TypeParameter.FILEREADWRITE || parameter.typeParameter == TypeParameter.FILEREAD) {
                milkJob.setParameterStream(parameter, bonitaProperties.getPropertyStream(milkJob.getId() + cstPrefixPropertiesStreamValue + parameter.name));
            }
        }
        logger.fine(".getInstanceFromBonitaProperties end");

        return milkJob;
    }

    public List<BEvent> dbLoadAllJobs() {
        return dbLoadAllJobsAndPurge(false);
    }

    public List<BEvent> dbLoadAllJobsAndPurge(boolean purge) {
        List<BEvent> listEvents = new ArrayList<BEvent>();
        bonitaProperties.setCheckDatabase(false);
        logger.fine(".dbLoadAllJobsAndPurge-begin");

        /** soon : name is NULL to load all tour */
        listEvents.addAll(bonitaProperties.loaddomainName(BONITAPROPERTIESDOMAIN));
        // bonitaProperties.traceInLog();
        List<String> listToursToDelete = new ArrayList<String>();
        Enumeration<?> enumKey = bonitaProperties.propertyNames();
        while (enumKey.hasMoreElements()) {
            String idTourSt = (String) enumKey.nextElement();
            // String plugInTourSt = (String) bonitaProperties.get(idTour);
            Long idTour = null;
            try {
                if (idTourSt.endsWith(prefixPropertiesAskStop))
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
            if (listStreamToDelete.size() > 0) {
                for (String idStream : listStreamToDelete) {
                    bonitaProperties.removeStream(idStream);
                }
            }
            keysToDelete.addAll(listToursToDelete);
            if (keysToDelete.size() > 0) {
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

    public List<BEvent> dbSaveAllJobs() {
        List<BEvent> listEvents = new ArrayList<>();
        bonitaProperties.setCheckDatabase(false);
        logger.info(".dbSaveAllJobs-begin ***");

        listEvents.addAll(bonitaProperties.loaddomainName(BONITAPROPERTIESDOMAIN));
        for (MilkJob milkJob : milkJobFactory.getMapJobsId().values()) {
            dbSaveMilkJob(milkJob, SaveJobParameters.getInstanceAllInformations());
        }
        logger.info(".dbSaveAllJobs-end");

        return listEvents;
    }

    /**
     * Do to asynchronous, when a user ask to stop, we must save it. In the same time, if the job report a parameters update, then we have to save it, but do
     * not override the previous update.
     */
    public static class SaveJobParameters {

        boolean saveFileRead = false;
        boolean saveFileWrite = false;
        boolean saveAskStop = false;
        boolean saveTrackExecution = false;
        boolean savedExecution = false;
        boolean saveBase = false;

        public static SaveJobParameters getInstanceTrackExecution() {
            SaveJobParameters saveParameters = new SaveJobParameters();
            saveParameters.saveTrackExecution = true;
            return saveParameters;
        }

        /**
         * sall all main information. End of an execution for example
         * 
         * @return
         */
        public static SaveJobParameters getInstanceAllInformations() {
            SaveJobParameters saveParameters = new SaveJobParameters();
            saveParameters.saveTrackExecution = true;
            saveParameters.saveAskStop = true;
            saveParameters.saveBase = true;
            return saveParameters;
        }

        public static SaveJobParameters getInstanceEverything() {
            SaveJobParameters saveParameters = new SaveJobParameters();
            saveParameters.saveTrackExecution = true;
            saveParameters.saveAskStop = true;
            saveParameters.saveBase = true;
            saveParameters.saveFileRead = true;
            saveParameters.saveFileWrite = true;

            return saveParameters;
        }

        public static SaveJobParameters getInstanceStartExecutionJob() {
            SaveJobParameters saveParameters = new SaveJobParameters();
            saveParameters.saveTrackExecution = true;
            saveParameters.savedExecution = false;
            saveParameters.saveAskStop = true;
            saveParameters.saveBase = false;
            saveParameters.saveFileRead = false;
            saveParameters.saveFileWrite = false;

            return saveParameters;
        }

        public static SaveJobParameters getInstanceEndExecutionJob() {
            SaveJobParameters saveParameters = new SaveJobParameters();
            saveParameters.saveTrackExecution = true;
            saveParameters.savedExecution = true;
            saveParameters.saveAskStop = true;
            saveParameters.saveBase = false;
            saveParameters.saveFileRead = false;
            saveParameters.saveFileWrite = true;

            return saveParameters;
        }

        /**
         * Save the based information. Do not save the execution / askstop information
         * 
         * @return
         */
        public static SaveJobParameters getBaseInformations() {
            SaveJobParameters saveParameters = new SaveJobParameters();
            saveParameters.saveBase = true;
            return saveParameters;
        }

        public static SaveJobParameters getAskStop() {
            SaveJobParameters saveParameters = new SaveJobParameters();
            saveParameters.saveAskStop = true;
            return saveParameters;
        }

    }

    /**
     * top let the properties purge only what is needed, each item must start by an unique name
     */
    public static String prefixPropertiesBase = "";
    private static String cstPrefixPropertiesStreamValue = "_s_";

    public static String prefixPropertiesAskStop = "_askStop";
    public static String prefixPropertiesTrackExecution = "_trackExec";
    public static String prefixPropertiesSavedExecution = "_savedExecution";

    //  public static String prefixPropertiesTrackStream = "_stream";

    public List<BEvent> dbSaveMilkJob(MilkJob milkJob, SaveJobParameters saveParameters) {
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

            bonitaProperties.put(String.valueOf(milkJob.getId()) + prefixPropertiesBase, getJsonSt( milkJob.getMap(mapContentParameter)));
            listKeys.add(milkJob.getId() + prefixPropertiesBase);
        }
        if (saveParameters.saveAskStop) {
            bonitaProperties.put(String.valueOf(milkJob.getId()) + prefixPropertiesAskStop, milkJob.isAskedForStop());
            listKeys.add(milkJob.getId() + prefixPropertiesAskStop);
        }
        if (saveParameters.savedExecution) {
            bonitaProperties.put(String.valueOf(milkJob.getId()) + prefixPropertiesSavedExecution, getJsonSt( milkJob.getMapListSavedExecution()));
            listKeys.add(milkJob.getId() + prefixPropertiesSavedExecution);
        }
        if (saveParameters.saveTrackExecution) {
            bonitaProperties.put(String.valueOf(milkJob.getId()) + prefixPropertiesTrackExecution, getJsonSt( milkJob.getTrackExecution()) );
            listKeys.add(milkJob.getId() + prefixPropertiesTrackExecution);
        }
        // save all
        PlugInDescription plugInDescription = milkJob.getPlugIn().getDescription();
        for (PlugInParameter parameter : plugInDescription.inputParameters) {
            boolean saveFile = false;
            if (saveParameters.saveFileWrite && (parameter.typeParameter == TypeParameter.FILEWRITE || parameter.typeParameter == TypeParameter.FILEREADWRITE))
                saveFile = true;
            if (saveParameters.saveFileRead && (parameter.typeParameter == TypeParameter.FILEREADWRITE || parameter.typeParameter == TypeParameter.FILEREAD))
                saveFile = true;
            if (saveFile) {
                bonitaProperties.setPropertyStream(milkJob.getId() + cstPrefixPropertiesStreamValue + parameter.name, milkJob.getParameterStream(parameter));
                listKeys.add(milkJob.getId() + cstPrefixPropertiesStreamValue + parameter.name);
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
        Set<String> listKeys = new HashSet<String>();
        listKeys.add(milkJob.getId() + prefixPropertiesBase);
        listKeys.add(milkJob.getId() + prefixPropertiesAskStop);
        listKeys.add(milkJob.getId() + prefixPropertiesTrackExecution);

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
    private final static String BONITAPROPERTIES_SCHEDULERTYPE = "schedulertype";

    public SerializeOperation getCurrentScheduler(MilkSchedulerFactory schedulerFactory) {
        SerializeOperation serializeOperation = new SerializeOperation();

        bonitaProperties.setCheckDatabase(false); // we check only when we deploy a new command
        serializeOperation.listEvents.addAll(bonitaProperties.loaddomainName(BONITAPROPERTIES_DOMAINSCHEDULER));
        String valueSt = (String) bonitaProperties.get(BONITAPROPERTIES_SCHEDULERTYPE);
        serializeOperation.scheduler = schedulerFactory.getFromType(valueSt, true);

        return serializeOperation;
    }

    public SerializeOperation saveCurrentScheduler(MilkSchedulerInt currentScheduler) {
        logger.fine(".saveCurrentScheduler-begin");
        Set<String> listKeys = new HashSet<>();

        SerializeOperation serializeOperation = new SerializeOperation();
        bonitaProperties.setCheckDatabase(false); // we check only when we deploy a new command
        serializeOperation.listEvents.addAll(bonitaProperties.loaddomainName(BONITAPROPERTIES_DOMAINSCHEDULER));
        
        bonitaProperties.put(BONITAPROPERTIES_SCHEDULERTYPE, currentScheduler.getType().toString());
        listKeys.add(BONITAPROPERTIES_SCHEDULERTYPE);
        
        serializeOperation.listEvents.addAll(bonitaProperties.storeCollectionKeys(listKeys));

        logger.fine(".saveCurrentScheduler-end");

        return serializeOperation;
    }

  

}
