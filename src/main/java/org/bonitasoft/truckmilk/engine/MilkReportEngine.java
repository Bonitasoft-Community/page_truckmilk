package org.bonitasoft.truckmilk.engine;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.properties.BonitaProperties;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

/**
 * This class is in charge to save the reportExecution engine
 * 
 * @author Firstname Lastname
 */

public class MilkReportEngine {

    private static MilkLog logger = MilkLog.getLogger(MilkReportEngine.class.getName());
    private final static String LOG_HEADER = "MilkReportEngine  ~~ ";

    private final static String BONITAPROPERTIES_DOMAIN_REPORT = "report";
    private final static String BONITAPROPERTIES_DOMAIN_HEARTBREAT = "heartbreath";
    
    // private final static String BONITAPROPERTIES_HEARTBREATH_LOGINFOBEAT = "schedulerLogInfoBeat";
    private final static String BONITAPROPERTIES_HEARTBEAT_NBSAVEDHEARTBEAT = "schedulerNbSavedHeartBeat";
    
    private final static String BONITAPROPERTIES_HEARTBEAT_SAVEDHEARTBEAT = "schedulersavedHeartBeat";
    
    private final static String BONITAPROPERTIES_REPORT = "reportline";
        

    private static MilkReportEngine reportEngine = new MilkReportEngine();

    public static MilkReportEngine getInstance() {
        return reportEngine;
    }

    
   

    /**
     * Saved in database this number of heartBeat
     */
    private long nbSavedHeartBeat = 1;
    private boolean isInitialised = false;

    public long getNbSavedHeartBeat() {
        return nbSavedHeartBeat;
    }

    public void setNbSavedHeartBeat(long nbHeartBeat) {
        if (nbHeartBeat > 3600)
            nbHeartBeat = 3600;
        if (nbSavedHeartBeat < 60)
            nbSavedHeartBeat = 60;
        this.nbSavedHeartBeat = nbHeartBeat;
    }

    /**
     * list of Report information
     */
    private List<String> listSaveHeartBeatInformation = new ArrayList<>();

    public List<String> getListSaveHeartBeatInformation() {
        return listSaveHeartBeatInformation;
    }

    /**
     * we want to keep the HeartBearthInformation separate. All operation can comes from anywhere in a cluster environment, we want to be sure to keep the hearthbreath and job execution
     * @param message
     * @param forceLog
     */
    public void reportHeartBeatInformation(String message, boolean doSaveInDatabase, boolean logHeartBeat) {        
        report(message, false, true, doSaveInDatabase, logHeartBeat );

    }
    
    /* public synchronized void addListSaveHeartBeatInformation(String message) {
        listSaveHeartBeatInformation.add(message);
    }
    */
    /* ******************************************************************************** */
    /*                                                                                  */
    /* Report Operation */
    /*                                                                                  */
    /* ******************************************************************************** */

    private List<String> listSaveReportInformation = new ArrayList<>();

    public synchronized void reportOperation(List<BEvent>listEvents) {
        StringBuilder listEventSt = new StringBuilder();
        for (BEvent event : listEvents) {
            listEventSt.append( event.getTitle());
            if (event.getParameters()!=null && ! event.getParameters().isEmpty())
                listEventSt.append(event.getParameters());
            listEventSt.append("; ");
        }
        report(listEventSt.toString(), true, false,true, false /* not a heart breath */ );
    }
    public List<String> getListReportOperation(){
        return listSaveReportInformation;
    }
   

    

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Internal method */
    /*                                                                                  */
    /* ******************************************************************************** */

    private   void reduceLists() {
        if (nbSavedHeartBeat < 60)
            nbSavedHeartBeat = 60;
        synchronized( listSaveHeartBeatInformation ) {
            while (listSaveHeartBeatInformation.size() > nbSavedHeartBeat)
                listSaveHeartBeatInformation.remove(0);
        }
            
        synchronized( listSaveReportInformation ) {   
            while (listSaveReportInformation.size() > 50)
                listSaveReportInformation.remove(0);
        }
        
    }

    /**
     * 
     * @param message
     * @param forceLog
     * @param saveHeartBeat
     * @param doSaveInDatabase : do not save everytime, to save execution CPU. Start and End of a job can wait for example the next heartbeat
     */
    private synchronized void report(String message, boolean forceLog, boolean saveHeartBeat, boolean doSaveInDatabase, boolean logHeartBeat) {
        
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM HH:mm:ss");
        message= sdf.format( new Date())+" "+message;
        
        if (saveHeartBeat)
            listSaveHeartBeatInformation.add(message);
        else
            listSaveReportInformation.add( message );
        
        
        if ( (logHeartBeat && saveHeartBeat) || forceLog)
            logger.info(LOG_HEADER +  message);
        else
            logger.fine(LOG_HEADER + message);
        // to not have an infinite or a negative loop,
        reduceLists();
        saveReport( saveHeartBeat, doSaveInDatabase );

    }
    /**
     * @return
     */
    private synchronized List<BEvent> saveReport(boolean saveHeartBeat, boolean doSaveInDatabase) {
        BonitaProperties bonitaProperties = new BonitaProperties(MilkSerializeProperties.BONITAPROPERTIESNAME);

        List<BEvent> listEvents = new ArrayList<>();

        bonitaProperties.setCheckDatabase(false); // we check only when we deploy a new command
        listEvents.addAll(bonitaProperties.loaddomainName(saveHeartBeat ? BONITAPROPERTIES_DOMAIN_REPORT : BONITAPROPERTIES_DOMAIN_HEARTBREAT));
        Set<String> listKeys = new HashSet<>();

        if (saveHeartBeat) {
        
            // maybe the first time after a startup? Then, in that situation, reread the value
            if ( !isInitialised) {
                nbSavedHeartBeat = getIntegerValue(bonitaProperties.get(BONITAPROPERTIES_HEARTBEAT_NBSAVEDHEARTBEAT), 60);
                
                synchronized( listSaveHeartBeatInformation ) {
                    loadExistingList(bonitaProperties, BONITAPROPERTIES_HEARTBEAT_SAVEDHEARTBEAT, listSaveHeartBeatInformation);
                }
                isInitialised = true;
    
            }
            // now saved
    
            bonitaProperties.put(BONITAPROPERTIES_HEARTBEAT_NBSAVEDHEARTBEAT, getNbSavedHeartBeat());
            listKeys.add(BONITAPROPERTIES_HEARTBEAT_NBSAVEDHEARTBEAT);
            
            /** save history */
            synchronized( listSaveHeartBeatInformation ) {
                saveList( bonitaProperties, listKeys, BONITAPROPERTIES_HEARTBEAT_SAVEDHEARTBEAT, listSaveHeartBeatInformation);
            }
        }
        
        else {
            synchronized( listSaveReportInformation ) {
                saveList( bonitaProperties, listKeys, BONITAPROPERTIES_REPORT, listSaveReportInformation);
            }
        }
        if (doSaveInDatabase)
            listEvents.addAll(bonitaProperties.storeCollectionKeys(listKeys));
        return listEvents;
    }

    /**
     * 
     * @param bonitaProperties
     * @param listName
     * @param memoryList
     * @return
     */
    private List<String> loadExistingList(BonitaProperties bonitaProperties, String listName, List<String> memoryList ) {
        
        // load the previous values. We may have then at this moment
        List<String> existingList = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            Object value = bonitaProperties.get(listName + "_" + i);
            if (value == null)
                break;
            existingList.add(value.toString());
        }
        memoryList.addAll(0, existingList);
        return memoryList;
    }
    
    private void saveList(BonitaProperties bonitaProperties, Set<String> listKeys, String listName, List<String> memoryList ) {
        for (int i = 0; i < memoryList.size(); i++) {
            bonitaProperties.put(listName + "_" + i, memoryList.get(i));
            listKeys.add(listName + "_" + i);
        }

    }
    /**
     * @param value
     * @param defaultValue
     * @return
     */
    private int getIntegerValue(Object value, int defaultValue) {
        if (value instanceof Integer)
            return ((Integer) value).intValue();
        try {
            return Integer.parseInt(value.toString());
        } catch (Exception e) {
            return defaultValue;
        }
    }

    

    private boolean getBooleanValue(Object value, boolean defaultValue) {
        if (value instanceof Boolean)
            return ((Boolean) value).booleanValue();
        try {
            return Boolean.parseBoolean(value.toString());
        } catch (Exception e) {
            return defaultValue;
        }
    }


}
