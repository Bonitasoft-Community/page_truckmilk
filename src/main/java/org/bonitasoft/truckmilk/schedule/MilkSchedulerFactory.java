package org.bonitasoft.truckmilk.schedule;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.engine.MilkSerializeProperties;
import org.bonitasoft.truckmilk.engine.MilkSerializeProperties.SerializeOperation;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt.StatusScheduler;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt.TypeScheduler;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt.TypeStatus;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

import groovyjarjarantlr.debug.Event;
import lombok.Data;

/**
 * this class manage the different scheduler, and give access to the scheduler selected
 */
public @Data class MilkSchedulerFactory {

    private static MilkLog logger = MilkLog.getLogger(MilkSchedulerFactory.class.getName());
    private final static String LOG_HEADER = "MilkSchedulerFactory  ~~ ";

    private static MilkSchedulerFactory milkSchedulerFactory = new MilkSchedulerFactory();

    private static BEvent eventUnknowSchedulerType = new BEvent(MilkSchedulerFactory.class.getName(), 1, Level.APPLICATIONERROR,
            "This Scheduler type is unknow", "This scheduler code is unknown", "The scheduler can't change", "Check the scheduler type");

    private static BEvent eventCantChangeScheduler = new BEvent(MilkSchedulerFactory.class.getName(), 2, Level.APPLICATIONERROR,
            "Scheduler doesn't change", "The scheduler can't change, due to an error", "The same scheduler is used", "Check previous error");

    private static BEvent eventSchedulerChanged = new BEvent(MilkSchedulerFactory.class.getName(), 3, Level.SUCCESS,
            "Scheduler change", "The scheduler change");

    private static BEvent eventNoScheduler = new BEvent(MilkSchedulerFactory.class.getName(), 4, Level.APPLICATIONERROR,
            "No Scheduler", "No scheduler are configured.", "No monitoring", "Select one");

    private static BEvent eventHeartBeat = new BEvent(MilkSchedulerFactory.class.getName(), 5, Level.INFO,
            "Heat beat information", "Last heart beat, and next one");

    private MilkSchedulerInt currentScheduler = null;

    private Map<String, MilkSchedulerInt> setOfScheduler = new HashMap<>();

    private MilkSchedulerFactory() {
        setOfScheduler.put(TypeScheduler.QUARTZ.toString(), new MilkScheduleQuartz(this));
        setOfScheduler.put(TypeScheduler.THREADSLEEP.toString(), new MilkScheduleThreadSleep(this));
    }

    public static MilkSchedulerFactory getInstance() {
        return milkSchedulerFactory;
    }

    /**
     * verify that everything is correct for the scheduler environment
     * 
     * @param tenantId
     * @return
     */
    public List<BEvent> checkEnvironment(long tenantId) {
        List<BEvent> listEvents = new ArrayList<>();
        for (String key : setOfScheduler.keySet()) {
            MilkSchedulerInt scheduler = setOfScheduler.get(key);
            listEvents.addAll(scheduler.check(tenantId));
        }

        return listEvents;
    }

    

    /**
     * read in the configuration the information
     * If no scheduler already exist, then we try to start a default scheduler, Quartz
     * Note: at the end of the startup, the currentScheduler may be null.
     */
    public StatusScheduler startup(long tenantId) {
        StatusScheduler startupResult = new StatusScheduler();
        if (currentScheduler == null) {
            synchronized (this) {
                // multi thread : check again, because now we are in the mono thread, and object may be created by an another thread
                if (currentScheduler == null) {
                    logger.info(LOG_HEADER+"No scheduler is defined, so choose and start the QuartzScheduler");
                    startupResult.message.append("Quart Scheduler choosen;");
                    MilkSchedulerInt startScheduler = new MilkScheduleQuartz(this);
                    List<BEvent> listEventsQuartz = startScheduler.check( tenantId);
                    startupResult.listEvents.addAll(listEventsQuartz);
                    if (BEventFactory.isError( listEventsQuartz)) {
                        // Quartz is not installed, do not need to go ones
                        startupResult.isSchedulerReady = false;
                        startupResult.status = TypeStatus.UNDEFINED;
                        startupResult.message.append("Not ready "+getMessageFromListEvents(listEventsQuartz));
                    } else {
                        startupResult.isSchedulerReady = true;
                        startupResult.isNewSchedulerChoosen = true;
                        // then let's start the Quartz
                        listEventsQuartz = startScheduler.startup(tenantId, true);
                        startupResult.listEvents.addAll(listEventsQuartz);
                        if (BEventFactory.isError( listEventsQuartz)) {
                            startupResult.status = TypeStatus.UNDEFINED;
                            startupResult.message.append("Can't start: "+getMessageFromListEvents(listEventsQuartz));                            
                        }
                        else {
                            startupResult.status = TypeStatus.STARTED;
                            startupResult.message.append("Started");
                            currentScheduler = startScheduler;
                        }
                    }
                    return startupResult;    
                }
            }
        }
        // if we are here, time to get the status
        return getStatus(tenantId);
    }

    public TypeScheduler getTypeScheduler() {
        return currentScheduler==null ? null : currentScheduler.getType();
    }

    /**
     * @return
     */
    public List<String> getListTypeScheduler() {
        List<String> listSchedulers = new ArrayList<>();
        for (String key : setOfScheduler.keySet()) {
            listSchedulers.add(key);
        }
        return listSchedulers;
    }

    /**
     * get the scheduler
     * 
     * @return
     */
    public MilkSchedulerInt getScheduler() {
        return currentScheduler;

    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Information on the running */
    /*                                                                                  */
    /* ******************************************************************************** */
    /**
     * information on the execution
     * 
     * @param tenantId
     */
    public Date lastExecution = null;

    public void informRunInProgress(long tenantId) {
        lastExecution = new Date();
    }

    public StatusScheduler getStatus(long tenantId) {
        StatusScheduler statusScheduler;
        Date nextDateHeartBeat = null;
        if (currentScheduler != null) {
            statusScheduler = currentScheduler.getStatus(tenantId);
            nextDateHeartBeat = currentScheduler.getDateNextHeartBeat(tenantId);
        } else {
            statusScheduler = new StatusScheduler();
            statusScheduler.status = TypeStatus.UNDEFINED; 
            statusScheduler.listEvents.add(eventNoScheduler);
        }
        if (!BEventFactory.isError(statusScheduler.listEvents)) {
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
            String message = "Last Heart Beat[" + (lastExecution == null ? "next minute" : sdf.format(lastExecution)) + "]";
            if (statusScheduler.status == TypeStatus.STARTED)
                message += ", Next[" + (nextDateHeartBeat == null ? "next minute" : sdf.format(nextDateHeartBeat)) + "]";
            else
                message += "Scheduler stopped;";

            statusScheduler.listEvents.add(new BEvent(eventHeartBeat, message));
        }
        return statusScheduler;

    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Change the type of scheduler now */
    /*
     * /*
     */
    /* ******************************************************************************** */
    public List<BEvent> changeScheduler(String typeSchedulerToChange, long tenantId, boolean saveNow) {
        List<BEvent> listEvents = new ArrayList<>();

        MilkSchedulerInt newScheduler = getFromType(typeSchedulerToChange);
        if (newScheduler == null) {
            listEvents.add(new BEvent(eventUnknowSchedulerType, "Scheduler[" + typeSchedulerToChange + "]"));
        } else {
            if (currentScheduler == null || !newScheduler.getType().equals(milkSchedulerFactory.getScheduler().getType())) {
                if (currentScheduler != null) {
                    listEvents.addAll(currentScheduler.shutdown(tenantId));
                }
                if (BEventFactory.isError(listEvents)) {
                    // can't change
                    listEvents.add(new BEvent(eventCantChangeScheduler, "Scheduler[" + typeSchedulerToChange + "]"));
                } else {
                    currentScheduler = newScheduler;
                    listEvents.add(eventSchedulerChanged);

                    listEvents.addAll(currentScheduler.startup(tenantId, true));
                }
            }

        }
        if (currentScheduler == null) {
            listEvents.add(new BEvent(eventCantChangeScheduler, "Scheduler[" + typeSchedulerToChange + "]"));
            return listEvents;
        }

        // save the new value?
        if (saveNow)
            listEvents.addAll(savedScheduler(tenantId));
        return listEvents;

    }

    /**
     * saved the current scheduler
     * 
     * @return
     */
    public List<BEvent> savedScheduler(long tenantId) {
        List<BEvent> listEvents = new ArrayList<>();
        
        if (currentScheduler==null)
            return listEvents;
        
        MilkSerializeProperties serializeProperties = new MilkSerializeProperties(tenantId);
        SerializeOperation serializeOperation = serializeProperties.saveCurrentScheduler(currentScheduler);
        listEvents.addAll(serializeOperation.listEvents);
        return listEvents;
    }

    /**
     * get the scheduler according the type, if it's exist, else return null
     * 
     * @param typeScheduler
     * @return
     */
    public MilkSchedulerInt getFromType(String typeScheduler) {
        
        if (setOfScheduler.containsKey(typeScheduler))
            return setOfScheduler.get(typeScheduler);
        return null;
    }

    
    private String getMessageFromListEvents( List<BEvent> listEvents) {
        StringBuilder message = new StringBuilder();
        for (BEvent event : listEvents )
            message.append( event.getTitle()+" "+event.getParameters()+";");
        return message.toString();
    }
   
}
