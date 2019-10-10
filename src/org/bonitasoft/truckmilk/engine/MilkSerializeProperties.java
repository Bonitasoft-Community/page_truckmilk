package org.bonitasoft.truckmilk.engine;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.properties.BonitaProperties;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.job.MilkJob;

/**
 * this class manage the serialization of the TOur in Bonita Properties
 * 
 * @author Firstname Lastname
 */
public class MilkSerializeProperties {

    BonitaProperties bonitaProperties;
    private static String cstPrefixStreamValue = "_st";

    public final static String BonitaPropertiesName = "MilkTour";
    private final static String BonitaPropertiesDomain = "tour";
    private final static String BonitaPropertiesMainInfo = "maininfo";
    private MilkJobFactory milkPlugInTourFactory;

    private static BEvent eventBadTourJsonFormat = new BEvent(MilkSerializeProperties.class.getName(), 1,
            Level.APPLICATIONERROR,
            "Bad tour format", "A Milk tour can't be read due to a Json format", "This tour is lost", "Reconfigure it");

    public static class SerializeOperation {

        public List<BEvent> listEvents = new ArrayList<BEvent>();
        public String valueSt;
    }

    public MilkSerializeProperties(MilkJobFactory milkPlugInTourFactory) {
        this.milkPlugInTourFactory = milkPlugInTourFactory;
        this.bonitaProperties = new BonitaProperties(BonitaPropertiesName, milkPlugInTourFactory.getTenantId());
    }

    public MilkSerializeProperties(long tenantId) {
        this.bonitaProperties = new BonitaProperties(BonitaPropertiesName, tenantId);
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
     * @param milkPlugInTourFactory
     * @return
     */
    private MilkJob getInstanceFromBonitaProperties(Long idJob) {
        String jobSt = (String) bonitaProperties.get(idJob.toString());
        MilkJob milkJobplugInTour = MilkJob.getInstanceFromJson(jobSt, milkPlugInTourFactory);
        if (milkJobplugInTour == null)
            return null; // not a normal way
        // load File Parameters
        PlugInDescription plugInDescription = milkJobplugInTour.getPlugIn().getDescription();
        for (PlugInParameter parameter : plugInDescription.inputParameters) {
            if (parameter.typeParameter == TypeParameter.FILEWRITE || parameter.typeParameter == TypeParameter.FILEREADWRITE || parameter.typeParameter == TypeParameter.FILEREAD) {
                milkJobplugInTour.setParameterStream(parameter, bonitaProperties.getPropertyStream(milkJobplugInTour.getId() + "_" + parameter.name));
            }
        }

        return milkJobplugInTour;
    }

    public List<BEvent> dbLoadAllJobs() {
        List<BEvent> listEvents = new ArrayList<BEvent>();
        bonitaProperties.setCheckDatabase(false);

        /** soon : name is NULL to load all tour */
        listEvents.addAll(bonitaProperties.loaddomainName(BonitaPropertiesDomain));
        List<String> listToursToDelete = new ArrayList<String>();
        Enumeration<?> enumKey = bonitaProperties.propertyNames();
        boolean newIdWasGenerated = false;
        while (enumKey.hasMoreElements()) {
            String idTour = (String) enumKey.nextElement();
            // String plugInTourSt = (String) bonitaProperties.get(idTour);
            MilkJob plugInTour = getInstanceFromBonitaProperties(Long.valueOf(idTour));

            if (plugInTour != null) {
                if (plugInTour.newIdGenerated)
                    newIdWasGenerated = true;
                // register in the factory now
                milkPlugInTourFactory.putJob(plugInTour);
            } else {
                listToursToDelete.add(idTour);
                listEvents.add(new BEvent(eventBadTourJsonFormat, "Id[" + idTour + "]"));
            }
        }
        if (listToursToDelete.size() > 0 || newIdWasGenerated) {
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
                if (!milkPlugInTourFactory.existJob(idTourL)) {
                    listStreamToDelete.add(idStream);
                }
            } catch (Exception e) {
            } ; // idTour is not a long... strange, then do nothing
        }

        if (listStreamToDelete.size() > 0) {
            for (String idStream : listStreamToDelete)
                bonitaProperties.removeStream(idStream);
        }

        // save it to delete all corrupt tour
        if (listToursToDelete.size() > 0 || newIdWasGenerated || listStreamToDelete.size() > 0)
            listEvents.addAll(bonitaProperties.store());

        return listEvents;
    }

    /**
     * @param idTour
     * @return
     */
    public MilkJob dbLoadJob(Long idJob) {
        bonitaProperties.setCheckDatabase(false);

        /** soon : name is NULL to load all tour */
        bonitaProperties.loaddomainName(BonitaPropertiesDomain);
      
        MilkJob milkJob = getInstanceFromBonitaProperties(idJob);
        // register in the factory now
        milkPlugInTourFactory.putJob(milkJob);

        if (milkJob.newIdGenerated) {
            bonitaProperties.put(milkJob.getId(), milkJob.getJsonSt());

            bonitaProperties.store();
        }

        return milkJob;
    }

    public List<BEvent> dbSaveAllJobs() {
        List<BEvent> listEvents = new ArrayList<BEvent>();
        bonitaProperties.setCheckDatabase(false);
        listEvents.addAll(bonitaProperties.loaddomainName(BonitaPropertiesDomain));
        for (MilkJob plugInTour : milkPlugInTourFactory.getMapJobsId().values()) {
            bonitaProperties.put(plugInTour.getId(), plugInTour.getJsonSt());
        }
        listEvents.addAll(bonitaProperties.store());

        return listEvents;
    }

    /**
     * ******************************************* Soon
     * 
     * @param plugInTour
     * @return
     */
    /*
     * public List<BEvent> dbSavePlugInTour(MilkPlugInTour plugInTour) {
     * List<BEvent> listEvents = new ArrayList<BEvent>();
     * bonitaProperties.setCheckDatabase(false);
     * listEvents.addAll(bonitaProperties.loaddomainName(String.valueOf( plugInTour.getId())));
     * bonitaProperties.put(BonitaPropertiesMainInfo, plugInTour.getJsonSt());
     * listEvents.addAll(bonitaProperties.store());
     * return listEvents;
     * }
     */

    public List<BEvent> dbSaveJob(MilkJob plugInTour, boolean saveFileRead) {
        List<BEvent> listEvents = new ArrayList<BEvent>();

        bonitaProperties.setCheckDatabase(false);
        listEvents.addAll(bonitaProperties.loaddomainName(BonitaPropertiesDomain));

        // save the plugInTour

        bonitaProperties.put(String.valueOf(plugInTour.getId()), plugInTour.getJsonSt());
        // save all
        PlugInDescription plugInDescription = plugInTour.getPlugIn().getDescription();
        for (PlugInParameter parameter : plugInDescription.inputParameters) {
            if (parameter.typeParameter == TypeParameter.FILEWRITE
                    || parameter.typeParameter == TypeParameter.FILEREADWRITE
                    || (saveFileRead && parameter.typeParameter == TypeParameter.FILEREAD)) {
                bonitaProperties.setPropertyStream(plugInTour.getId() + "_" + parameter.name, plugInTour.getParameterStream(parameter));
            }
        }
        listEvents.addAll(bonitaProperties.store());

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

    public List<BEvent> dbRemovePlugInTour(MilkJob plugInTour) {
        List<BEvent> listEvents = new ArrayList<BEvent>();
        bonitaProperties.setCheckDatabase(false);

        listEvents.addAll(bonitaProperties.loaddomainName(BonitaPropertiesDomain));

        // remove(pluginTour.getId() does not work, so let's compare the key
        bonitaProperties.remove(String.valueOf(plugInTour.getId()));

        listEvents.addAll(bonitaProperties.store());

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
        SerializeOperation serializeOperation = new SerializeOperation();

        serializeOperation.listEvents.addAll(bonitaProperties.loaddomainName(BonitaPropertiesDomainScheduler));
        bonitaProperties.put(BonitaPropertiesSchedulerType, typeScheduler);
        serializeOperation.listEvents.addAll(bonitaProperties.store());
        return serializeOperation;
    }
}
