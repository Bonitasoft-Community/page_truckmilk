package org.bonitasoft.truckmilk.engine;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.properties.BonitaProperties;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.job.MilkJob.MapContentParameter;
import org.bonitasoft.truckmilk.schedule.MilkScheduleQuartz;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

/**
 * this class manage the serialization of the TOur in Bonita Properties
 * 
 * @author Firstname Lastname
 */
public class MilkSerializeProperties {

    private static MilkLog logger =  MilkLog.getLogger(MilkScheduleQuartz.class.getName());
    

    /*
     * we can use multiple bonitaProperties at a time (multithread), assuming the store() is a transaction in the database and protect multiple write
     */
    BonitaProperties bonitaProperties;
 
    public final static String BonitaPropertiesName = "MilkTour";
    private final static String BonitaPropertiesDomain = "tour";
    // private final static String BonitaPropertiesMainInfo = "maininfo";
    private MilkJobFactory milkJobFactory;

    private static BEvent eventBadTourJsonFormat = new BEvent(MilkSerializeProperties.class.getName(), 1,
            Level.APPLICATIONERROR,
            "Bad tour format", "A Milk tour can't be read due to a Json format", "This tour is lost", "Reconfigure it");

    public static class SerializeOperation {

        public List<BEvent> listEvents = new ArrayList<BEvent>();
        public String valueSt;
    }

    public MilkSerializeProperties(MilkJobFactory milkPlugInTourFactory) {
        this.milkJobFactory = milkPlugInTourFactory;
        bonitaProperties = new BonitaProperties(BonitaPropertiesName, milkPlugInTourFactory.getTenantId());
        bonitaProperties.setLogLevel(java.util.logging.Level.FINE);
        
    }

    public MilkSerializeProperties(long tenantId) {
        bonitaProperties = new BonitaProperties(BonitaPropertiesName, tenantId);
        bonitaProperties.setLogLevel(java.util.logging.Level.FINE);
    }

    public List<BEvent> checkAndUpdateEnvironment() {
        List<BEvent> listEvents = new ArrayList<BEvent>();
        bonitaProperties.setCheckDatabase(true);
        listEvents.addAll(bonitaProperties.loaddomainName(BonitaPropertiesDomain));
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
    private MilkJob getInstanceFromBonitaProperties(Long jobId) {
        String jobSt = (String) bonitaProperties.get(jobId.toString());
        logger.info( ".getInstanceFromBonitaProperties begin Read JobId["+jobId+"] jsonst="+jobSt);
        
        MilkJob milkJob = MilkJob.getInstanceFromJson(jobSt, milkJobFactory);
        if (milkJob == null) {
            logger.info(".getInstanceFromBonitaProperties end because milJob=null ?");
            return null; // not a normal way
        }
        
        //-----------------------  read the another information
          
        String jsonStTrackExecution = (String) bonitaProperties.get(jobId.toString()+prefixPropertiesTrackExecution);
        if (jsonStTrackExecution != null)
            milkJob.trackExecution.readFromJson(jsonStTrackExecution);
        
        // askstop has its own variable, so read it after the track execution
        Object askStopObj = bonitaProperties.get(jobId.toString()+prefixPropertiesAskStop);
        if (askStopObj !=null)
            milkJob.trackExecution.askForStop = Boolean.valueOf(askStopObj.toString());
   
        String jsonStSavedExecution = (String) bonitaProperties.get(jobId.toString()+prefixPropertiesSavedExecution);
        if (jsonStSavedExecution != null)
            milkJob.readSavedExecutionFromJson(jsonStSavedExecution);

        
        // load File Parameters
        PlugInDescription plugInDescription = milkJob.getPlugIn().getDescription();
        for (PlugInParameter parameter : plugInDescription.inputParameters) {
            if (parameter.typeParameter == TypeParameter.FILEWRITE || parameter.typeParameter == TypeParameter.FILEREADWRITE || parameter.typeParameter == TypeParameter.FILEREAD) {
                milkJob.setParameterStream(parameter, bonitaProperties.getPropertyStream(milkJob.getId() + cstPrefixPropertiesStreamValue + parameter.name));
            }
        }
        logger.info( ".getInstanceFromBonitaProperties end");

        return milkJob;
    }

    public List<BEvent> dbLoadAllJobs() {
        return dbLoadAllJobsAndPurge( false );
    }
    public List<BEvent> dbLoadAllJobsAndPurge( boolean purge) {
        List<BEvent> listEvents = new ArrayList<BEvent>();
        bonitaProperties.setCheckDatabase(false);
        logger.info( ".dbLoadAllJobsAndPurge-begin");
        
        /** soon : name is NULL to load all tour */
        listEvents.addAll(bonitaProperties.loaddomainName(BonitaPropertiesDomain));
       // bonitaProperties.traceInLog();
        List<String> listToursToDelete = new ArrayList<String>();
        Enumeration<?> enumKey = bonitaProperties.propertyNames();
        while (enumKey.hasMoreElements()) {
            String idTourSt = (String) enumKey.nextElement();
            // String plugInTourSt = (String) bonitaProperties.get(idTour);
            Long idTour=null;
            try
            {
                idTour = Long.valueOf(idTourSt);
            }
            catch(Exception e)
            {                
            }
            if (idTour==null)
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
        
       if (purge)
       {
           Set<String> keysToDelete = new HashSet<String>();
           keysToDelete.addAll(listToursToDelete);
            if (listToursToDelete.size() > 0 ) {
                for (String idTour : listToursToDelete)
                    bonitaProperties.remove(idTour);
            }
    
            // check the properties name
            Enumeration<?> enumStream = bonitaProperties.propertyStream();
            List<String> listStreamToDelete = new ArrayList<String>();
    
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
                for (String idStream : listStreamToDelete)
                {
                    bonitaProperties.removeStream(idStream);
                }
            }
            keysToDelete.addAll(listToursToDelete);
            if (keysToDelete.size()>0)
            {
                // do the deletion now
                logger.info( ".dbLoadAllJobsAndPurge Delete Key and Stream ["+keysToDelete+"]");
                bonitaProperties.storeCollectionKeys(keysToDelete);
            }
       }
        logger.info( ".dbLoadAllJobsAndPurge-end");

        return listEvents;
    }

    /**
     * @param idTour
     * @return
     */
    public MilkJob dbLoadJob(Long jobId) {
        bonitaProperties.setCheckDatabase(false);
        
        logger.info( ".dbLoadJob - begin");

        /** soon : name is NULL to load all tour */
        bonitaProperties.loaddomainName(BonitaPropertiesDomain);
      
        MilkJob milkJob = getInstanceFromBonitaProperties(jobId);
        // register in the factory now
        milkJobFactory.putJob(milkJob);

        logger.info( ".dbLoadJob - end");

        return milkJob;
    }

    public List<BEvent> dbSaveAllJobs() {
        List<BEvent> listEvents = new ArrayList<BEvent>();
        bonitaProperties.setCheckDatabase(false);
        logger.info( ".dbSaveAllJobs-begin ***");

        listEvents.addAll(bonitaProperties.loaddomainName(BonitaPropertiesDomain));
        for (MilkJob milkJob : milkJobFactory.getMapJobsId().values()) {
            dbSaveMilkJob( milkJob, SaveJobParameters.getInstanceAllInformations() );
        }
        logger.info( ".dbSaveAllJobs-end");

        return listEvents;
    }

  
    /**
     * Do to asynchronous, when a user ask to stop, we must save it. In the same time, if the job report a parameters update, then we have to save it, but do not override the previous update.

     *
     */
    public static class SaveJobParameters {
        boolean saveFileRead = false;
        boolean saveFileWrite = false;
        boolean saveAskStop=false;
        boolean saveTrackExecution=false;
        boolean savedExecution=false;
        boolean saveBase=false;
        
        public static SaveJobParameters getInstanceTrackExecution() {
            SaveJobParameters saveParameters = new SaveJobParameters();
            saveParameters.saveTrackExecution = true;
            return saveParameters;
        }
        /**
         * sall all main information. End of an execution for example
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
    public static String prefixPropertiesBase ="";
    private static String cstPrefixPropertiesStreamValue = "_s_";

    public static String prefixPropertiesAskStop = "_askStop";
    public static String prefixPropertiesTrackExecution = "_trackExec";
    public static String prefixPropertiesSavedExecution= "_savedExecution";
    
   //  public static String prefixPropertiesTrackStream = "_stream";
    
    public List<BEvent> dbSaveMilkJob(MilkJob milkJob, SaveJobParameters saveParameters) {
        List<BEvent> listEvents = new ArrayList<BEvent>();
    
        bonitaProperties.setCheckDatabase(false);
        listEvents.addAll(bonitaProperties.loaddomainName(BonitaPropertiesDomain));

        Set<String> listKeys=new HashSet<String>();
        // save the job
        if (saveParameters.saveBase) {
            MapContentParameter mapContentParameter = new MapContentParameter();
            mapContentParameter.askStop= false;
            mapContentParameter.savedExecution= false;
            mapContentParameter.trackExecution= false;

            bonitaProperties.put(String.valueOf(milkJob.getId())+prefixPropertiesBase, milkJob.getJsonSt(mapContentParameter));
            listKeys.add( milkJob.getId()+prefixPropertiesBase) ;
        } 
        if (saveParameters.saveAskStop) {
            bonitaProperties.put(String.valueOf(milkJob.getId())+prefixPropertiesAskStop, milkJob.trackExecution.askForStop);
            listKeys.add( milkJob.getId()+prefixPropertiesAskStop) ;
          } 
        if (saveParameters.savedExecution) {
            bonitaProperties.put(String.valueOf(milkJob.getId())+prefixPropertiesSavedExecution, milkJob.getListSavedExecutionJsonSt());
            listKeys.add( milkJob.getId()+prefixPropertiesSavedExecution) ;
          } 
        if (saveParameters.saveTrackExecution) {
            bonitaProperties.put(String.valueOf(milkJob.getId())+prefixPropertiesTrackExecution, milkJob.trackExecution.getJsonSt());
            listKeys.add( milkJob.getId()+prefixPropertiesTrackExecution) ;
        }
        // save all
        PlugInDescription plugInDescription = milkJob.getPlugIn().getDescription();
        for (PlugInParameter parameter : plugInDescription.inputParameters) {
            boolean saveFile=false;
            if (saveParameters.saveFileWrite && (parameter.typeParameter == TypeParameter.FILEWRITE || parameter.typeParameter == TypeParameter.FILEREADWRITE))
                saveFile=true;
            if ( saveParameters.saveFileRead && (parameter.typeParameter == TypeParameter.FILEREADWRITE || parameter.typeParameter == TypeParameter.FILEREAD)) 
                saveFile=true;
            if (saveFile) {
                bonitaProperties.setPropertyStream(milkJob.getId() +cstPrefixPropertiesStreamValue + parameter.name, milkJob.getParameterStream(parameter));
                listKeys.add(milkJob.getId() +cstPrefixPropertiesStreamValue + parameter.name);
            }
        }
        logger.info(".dbSaveJob-begin keys="+listKeys.toString());

        listEvents.addAll(bonitaProperties.storeCollectionKeys(listKeys));
        logger.info( ".dbSaveJob-end");

        return listEvents;
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
        List<BEvent> listEvents = new ArrayList<BEvent>();
        bonitaProperties.setCheckDatabase(false);

        listEvents.addAll(bonitaProperties.loaddomainName(BonitaPropertiesDomain));
        logger.info( ".dbRemovePlugInTour-begin idJob="+milkJob.getId());

        // remove(pluginTour.getId() does not work, so let's compare the key
        Set<String> listKeys=new HashSet<String>();
        listKeys.add( milkJob.getId()+prefixPropertiesBase) ;
        listKeys.add( milkJob.getId()+prefixPropertiesAskStop) ;
        listKeys.add( milkJob.getId()+prefixPropertiesTrackExecution) ;
        
        for (String key : listKeys)
            bonitaProperties.remove( key );
        
        listEvents.addAll(bonitaProperties.storeCollectionKeys(listKeys));
        logger.info( ".dbRemovePlugInTour-end");

        return listEvents;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Save Scheduler information */
    /*                                                                                  */
    /* ******************************************************************************** */

    private final static String BonitaPropertiesDomainScheduler = "scheduler";
    private final static String BonitaPropertiesSchedulerType = "schedulertype";

    public SerializeOperation getCurrentScheduler() {
        SerializeOperation serializeOperation = new SerializeOperation();

        bonitaProperties.setCheckDatabase(false); // we check only when we deploy a new command
        serializeOperation.listEvents.addAll(bonitaProperties.loaddomainName(BonitaPropertiesDomainScheduler));
        serializeOperation.valueSt = (String) bonitaProperties.get(BonitaPropertiesSchedulerType);
        return serializeOperation;
    }

    public SerializeOperation saveCurrentScheduler(String typeScheduler) {
        logger.info( ".saveCurrentScheduler-begin");

        SerializeOperation serializeOperation = new SerializeOperation();
        bonitaProperties.setCheckDatabase(false); // we check only when we deploy a new command
        serializeOperation.listEvents.addAll(bonitaProperties.loaddomainName(BonitaPropertiesDomainScheduler));
        bonitaProperties.put(BonitaPropertiesSchedulerType, typeScheduler);
        
        Set<String> listKeys=new HashSet<String>();
        listKeys.add(BonitaPropertiesSchedulerType) ;
        serializeOperation.listEvents.addAll(bonitaProperties.storeCollectionKeys(listKeys));
        
        logger.info( ".saveCurrentScheduler-end");
        
        
        return serializeOperation;
    }
    
  
}
