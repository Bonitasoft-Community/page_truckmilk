package org.bonitasoft.truckmilk.engine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkSerializeProperties.SerializationJobParameters;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;

public class MilkJobFactory {

    public static BEvent EVENT_JOB_NOT_FOUND = new BEvent(MilkJobFactory.class.getName(), 3, Level.APPLICATIONERROR,
            "Job not found",
            "The job is not found, the operation is not performed. Maybe someone else delete it before you?",
            "Operation is not done", "Refresh the page");

    public static BEvent EVENT_JOB_ALREADY_EXIST = new BEvent(MilkJobFactory.class.getName(), 2, Level.APPLICATIONERROR,
            "Job already exist", "The job already exist with this name, a job must have a uniq name",
            "Job is not registered", "Choose a different name");
    public static BEvent EVENT_JOB_WITHOUT_NAME = new BEvent(MilkJobFactory.class.getName(), 3, Level.APPLICATIONERROR,
            "A job must have a name", "The job does not have a name, it can't be registered",
            "Job is not registered", "Give a name");

    // private static MilkPlugInTourFactory milkPlugInTourFactory = new MilkPlugInTourFactory();
    private MilkPlugInFactory milkPlugInFactory = null;
    private List<BEvent> listInitialiseEvent = new ArrayList<>();

    /**
     * this class is in charge to load/save tour
     */
    private MilkSerializeProperties milkSerialize;

    public static MilkJobFactory getInstance(MilkPlugInFactory milkPlugInFactory) {
        MilkJobFactory milkJobFactory = new MilkJobFactory(milkPlugInFactory);
        milkJobFactory.listInitialiseEvent = milkJobFactory.dbLoadAllJobs();

        return milkJobFactory;
    }

    /**
     * return the different factory but not load any information on.
     * 
     * @param milkPlugInFactory
     * @param noLoading
     * @return
     */
    public static MilkJobFactory getInstance(MilkPlugInFactory milkPlugInFactory, boolean noLoading) {
        MilkJobFactory milkJobFactory = new MilkJobFactory(milkPlugInFactory);
        return milkJobFactory;
    }

    private MilkJobFactory(MilkPlugInFactory milkPlugInFactory) {
        this.milkPlugInFactory = milkPlugInFactory;
        milkSerialize = new MilkSerializeProperties(this);

    }

    public List<BEvent> getInitialiseStatus() {
        return listInitialiseEvent;
    }

    public  MilkJobContext getMilkJobContext() {
        return milkPlugInFactory.getMilkJobContext();
    }

    /**
     * return the PLUGIN factory
     * 
     * @return
     */
    public MilkPlugInFactory getMilkPlugInFactory() {
        return milkPlugInFactory;
    };

    // keep the list of all tour
    Map<Long, MilkJob> listJobsId = new HashMap<>();

    /*
     * Map<String, MilkPlugInTour> listToursName = new HashMap<String, MilkPlugInTour>();
     * public MilkPlugInTour getByName(String name)
     * {
     * return listToursName.get( name==null ? MilkPlugInTour.DEFAULT_NAME : name );
     * }
     * public Map<String, MilkPlugInTour> getMapTourName()
     * {
     * return listToursName;
     * }
     */
   

    public Map<Long, MilkJob> getMapJobsId() {
        return listJobsId;
    }
    /**
     * return the job Index by the Id
     * 
     * @param name
     * @return
     */

    public MilkJob getJobById(Long id) {
        return listJobsId.get(id);
    }

    public Collection<MilkJob> getListJobs() {
        return getMapJobsId().values();
    }

    /**
     * remove the tour
     * 
     * @param idTour
     * @param tenantId
     * @return
     */
    public List<BEvent> removeJob(MilkJob milkJob, MilkJobExecution jobExecution) {
        List<BEvent> listEvents = new ArrayList<>();
        if (!listJobsId.containsKey(milkJob.getId())) {
            listEvents.add(new BEvent(EVENT_JOB_NOT_FOUND, "Job[" + milkJob.getId() + "]"));
            return listEvents;
        }

        
        milkJob.getPlugIn().notifyUnregisterAJob(milkJob, jobExecution);
        
        listEvents.addAll( dbRemoveJob(listJobsId.get( milkJob.getId())));
        listJobsId.remove( milkJob.getId());
        return listEvents;
    }

    public synchronized List<BEvent> registerAJob(MilkJob milkJob, MilkJobExecution milkJobExecution) {
        List<BEvent> listEvents = new ArrayList<>();
        // name must exist
        if (milkJob.getName() == null || milkJob.getName().trim().length() == 0) {
            listEvents.add(EVENT_JOB_WITHOUT_NAME);
            return listEvents;
        }
        // name must be unique
        for (MilkJob plugI : listJobsId.values()) {
            if (plugI.getName().equals(milkJob.getName())) {
                listEvents.add(new BEvent(EVENT_JOB_ALREADY_EXIST, milkJob.getName()));
                return listEvents;
            }
        }
        milkJob.getPlugIn().notifyRegisterAJob(milkJob, milkJobExecution);
        listJobsId.put(milkJob.getId(), milkJob);
        return listEvents;
    }

    /**
     * direct register, during a load for example
     * 
     * @param plugInTour
     */
    protected void putJob(MilkJob plugInTour) {
        listJobsId.put(plugInTour.getId(), plugInTour);
    }

    protected boolean existJob(long idTour) {
        return listJobsId.containsKey(idTour);
    }

    public class CreateJobStatus {

        List<BEvent> listEvents;
        MilkJob job;
    }

    /**
     * create a tour from a plug in
     * 
     * @param tourName
     * @param plugIn
     * @return
     */
    public CreateJobStatus createMilkJob(String jobName, MilkPlugIn plugIn, MilkJobExecution milkJobExecution) {
        CreateJobStatus createJobStatus = new CreateJobStatus();

        createJobStatus.job = MilkJob.getInstanceFromPlugin(jobName, plugIn, this);
        createJobStatus.listEvents = registerAJob(createJobStatus.job, milkJobExecution);

        return createJobStatus;
    }
    /* ******************************************************************************** */
    /*                                                                                  */
    /* Check */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    /**
     * this method to check is perform after the deployment, or in demand
     * 
     * @param tenantId
     * @return
     */
    public List<BEvent> checkAndUpdateEnvironment( MilkJobContext milkJobContext) {
        return milkSerialize.checkAndUpdateEnvironment();
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Load / Save */
    /*                                                                                  */
    /* Load and save are dedicated to the Factory, to group here the way to use the */
    /* external usage. */
    /* Decomposition : */
    /* name(BonitaPropertie) is BonitaPropertiesName */
    /* DomainName : the plugInTour Name */
    /* "key": "parameters" is the main parameters, in JSON */
    /* <inputName> : if the parameters is an InputStream */
    /*                                                                                  */
    /* ******************************************************************************** */
    // read all tours
    public List<BEvent> dbLoadAllJobs() {
        return milkSerialize.dbLoadAllJobs();

    }

    /**
     * save all tour
     * 
     * @param tenantId
     * @return
     */
    public List<BEvent> dbSaveAllJobs() {
        return milkSerialize.dbSaveAllJobs( getMilkJobContext());
    }

    /**
     * Soon
     * 
     * @param plugInTour
     * @return
     */
    /*
     * public List<BEvent> dbSavePlugInTour(MilkPlugInTour plugInTour) {
     * return milkSerialize.dbSavePlugInTour(plugInTour);
     * }
     */

    /**
     * load a particular plugtour
     */
    public static class MilkFactoryOp {

        public MilkJob job;
        public List<BEvent> listEvents = new ArrayList<>();
    }

    /**
     * load a particular plugtour
     * 
     * @param idTour Unique ID
     * @param tenantid tenantId to read the tour
     * @param MilkCmdControl : this parameter may be null if the call is done OUT of a command.
     * @return
     */
    public MilkFactoryOp dbLoadJob(Long idJob) {
        MilkFactoryOp milkFactoryOp = new MilkFactoryOp();
        milkFactoryOp.job = milkSerialize.dbLoadJob(idJob);
        return milkFactoryOp;

    }
    public MilkFactoryOp dbLoadPartialJob(Long idJob) {
        MilkFactoryOp milkFactoryOp = new MilkFactoryOp();
        milkFactoryOp.job = milkSerialize.dbLoadJob(idJob);
        return milkFactoryOp;

    }
    /**
     * save a particular plug tour
     * 
     * @param milkJob
     * @param saveFileRead : this parameters can be saved only on special occasion, when the tour is updated by the administrator. In any other circumstance, it
     *        has to be protected.
     * @return
     */
    public List<BEvent> dbSaveJob(MilkJob job, SerializationJobParameters saveParameters) {
        return milkSerialize.dbSaveMilkJob(job, getMilkJobContext(), saveParameters);
    }

    /**
     * remove a particular plug tour
     * 
     * @param tour
     * @return
     */
    private List<BEvent> dbRemoveJob(MilkJob milkJob) {
        return milkSerialize.dbRemoveMilkJob(milkJob);
    }

    // load all tour execution

}
