package org.bonitasoft.truckmilk.schedule;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bonitasoft.ext.properties.BonitaProperties;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt.StatusScheduler;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt.TypeScheduler;
import org.bonitasoft.truckmilk.tour.MilkCmdControl;
import org.bonitasoft.truckmilk.tour.MilkCmdControlAPI;
import org.bonitasoft.truckmilk.tour.MilkPlugInTourFactory;
import org.quartz.ScheduleBuilder;

/**
 * this class manage the different scheduler, and give access to the scheduler selected
 *
 */
public class MilkSchedulerFactory {

    private static MilkSchedulerFactory milkSchedulerFactory = new MilkSchedulerFactory();

    
    private static BEvent eventUnknowSchedulerType = new BEvent(MilkSchedulerFactory.class.getName(), 1, Level.APPLICATIONERROR,
            "This Scheduler type is unknow", "This scheduler code is unknown", "The scheduler can't change", "Check the scheduler type");
    
    
    private static BEvent eventCantChangeScheduler = new BEvent(MilkSchedulerFactory.class.getName(), 2, Level.APPLICATIONERROR,
            "Scheduler doesn't change" , "The scheduler can't change, due to an error", "The same scheduler is used", "Check previous error");
    
    private static BEvent eventSchedulerChanged = new BEvent(MilkSchedulerFactory.class.getName(), 3, Level.SUCCESS,
            "Scheduler change" , "The scheduler change");

    private static BEvent eventNoScheduler = new BEvent(MilkSchedulerFactory.class.getName(), 4, Level.APPLICATIONERROR,
            "No Scheduler" , "No scheduler are configured.", "No monitoring", "Select one");
    
    private static BEvent EVENT_HEART_BEAT = new BEvent(MilkSchedulerFactory.class.getName(), 5, Level.INFO,
            "Heat beat information" , "Last heart beat, and next one");
    
    private final static String BonitaPropertiesDomainScheduler="scheduler";
    private final static String BonitaPropertiesSchedulerType="schedulertype";
    
    private MilkSchedulerInt currentScheduler= null;
    
    public static MilkSchedulerFactory getInstance()
    {
        return milkSchedulerFactory;
    }
    
    /** read in the configuration the information */
    public List<BEvent> startup(long tenantId)
    {
        List<BEvent> listEvents= new ArrayList<BEvent>();
        if (currentScheduler ==null)
        {
            synchronized( this )
            {
                if (currentScheduler==null)
                {
                    // save the new value
                    BonitaProperties bonitaProperties = new BonitaProperties(MilkPlugInTourFactory.BonitaPropertiesName, tenantId);
                    listEvents.addAll(bonitaProperties.loaddomainName(BonitaPropertiesDomainScheduler));
                    String typeSchedulerSt = (String) bonitaProperties.get(BonitaPropertiesSchedulerType);
                    currentScheduler= getFromType(typeSchedulerSt);
                    
                    if (currentScheduler == null)
                    {
                        // we force the SchedulerQuartz
                        MilkSchedulerInt quartz = new MilkScheduleQuartz();
                        currentScheduler= quartz;
                    }
                }
            }
        }
        return listEvents;
    }
    
    
    public TypeScheduler getTypeScheduler()
    { return currentScheduler.getType();
    }

    /**
     * 
     * @return
     */
    public List<String> getListTypeScheduler()
    {
        List<String> listSchedulers= new ArrayList<String>();
        listSchedulers.add( TypeScheduler.QUARTZ.toString());
        listSchedulers.add( TypeScheduler.THREADSLEEP.toString());
        return listSchedulers;
    }
    
    /**
     * get the scheduler
     * @return
     */
    public MilkSchedulerInt getScheduler()
    {
        return currentScheduler;

    }
    
    
    /* ******************************************************************************** */
    /*                                                                                  */
    /* Information on the running                                                       */
    /*                                                                                  */
    /* ******************************************************************************** */
    /**
     * information on the execution
     * @param tenantId
     */
    public Date lastExecution = null;
    public void informRunInProgress(long tenantId)
    {
        lastExecution = new Date();
    }
    public StatusScheduler getStatus( long tenantId)
    {
        StatusScheduler statusScheduler;
        Date nextDateHeartBeat =null;
        if (currentScheduler!=null)
        {
            statusScheduler = currentScheduler.getStatus(tenantId);
            nextDateHeartBeat = currentScheduler.getDateNextHeartBeat(tenantId);
        }
        else
        {
            statusScheduler = new StatusScheduler();
            statusScheduler.listEvents.add( eventNoScheduler );
        }
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        String message= "Last Heart Beat["+ (lastExecution==null ? "undefined": sdf.format(lastExecution))+"]";
        message+=", Next["+(nextDateHeartBeat==null ? "undefined": sdf.format(nextDateHeartBeat))+"]";
        statusScheduler.listEvents.add( new BEvent( EVENT_HEART_BEAT, message));
        return statusScheduler;
                
    }
    
    /* ******************************************************************************** */
    /*                                                                                  */
    /* Change the type of scheduler now                                                 */
    /*
     * /*
     */
    /* ******************************************************************************** */
    public List<BEvent> changeTypeScheduler(String typeSchedulerToChange, long tenantId )
    {
        List<BEvent> listEvents= new ArrayList<BEvent>();
        
        MilkSchedulerInt newScheduler= getFromType(typeSchedulerToChange);
       
        if (newScheduler==null)
        {
            listEvents.add( new BEvent( eventUnknowSchedulerType, "Scheduler["+typeSchedulerToChange+"]"));
            return listEvents;
        }
        if (currentScheduler!=null)
        {
            listEvents.addAll( currentScheduler.shutdown(tenantId));
        }
        if (BEventFactory.isError(listEvents))
        {
         // can't change
            listEvents.add( new BEvent( eventCantChangeScheduler, "Scheduler["+typeSchedulerToChange+"]"));
            return listEvents; 
        }
        currentScheduler=newScheduler;
        listEvents.add(eventSchedulerChanged);
        
        listEvents.addAll(currentScheduler.startup(tenantId, true ));

        
        // save the new value
        BonitaProperties bonitaProperties = new BonitaProperties(MilkPlugInTourFactory.BonitaPropertiesName, tenantId);
        listEvents.addAll(bonitaProperties.loaddomainName(BonitaPropertiesDomainScheduler));
        bonitaProperties.put(BonitaPropertiesSchedulerType, currentScheduler.getType().toString());
        listEvents.addAll(bonitaProperties.store());
        return listEvents;
        
    }
    
    private MilkSchedulerInt getFromType(String typeScheduler)
    {
    if (TypeScheduler.QUARTZ.toString().equalsIgnoreCase(typeScheduler))
        return new MilkScheduleQuartz();
    
    if (TypeScheduler.THREADSLEEP.toString().equalsIgnoreCase(typeScheduler))
        return new MilkScheduleThreadSleep();
    return null;
    }
}
