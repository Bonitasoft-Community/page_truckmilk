package org.bonitasoft.truckmilk.schedule;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt.TypeStatus;

public abstract class MilkSchedulerInt {
 
    protected MilkSchedulerFactory factory;
    public MilkSchedulerInt( MilkSchedulerFactory factory ) {
        this.factory = factory;
    }
    
    public MilkSchedulerFactory getFactory() {
        return factory;
    }
    
    /**
     * return true if the scheduler need to be restarted each time a user access to the page for the first time
     * @return
     */
    public abstract boolean needRestartAtInitialization();
    /**
     * is the double start on a cluster is protected by the scheduler used ?
     * @return
     */
    public abstract boolean isClusterProtected();
    
  
 
        
    /**
     * initialize. Then the schedule must call back the engine on the method
     * milkCmdControl.executeOneTime( tenantId );
     * 
     * @param tenantId
     * @param forceReset
     * @return
     */
    public abstract List<BEvent> startup(long tenantId, boolean forceReset);

    public abstract List<BEvent> shutdown(long tenantId);

    /**
     * true if the scheduler is running (controled by start / stop
     * 
     * @return
     */
    public enum TypeStatus {
        STOPPED, SHUTDOWN, STARTED, UNDEFINED
    }

    public static final class StatusScheduler {

        public TypeStatus status;
        public boolean isSchedulerReady = false;
        public List<BEvent> listEvents = new ArrayList<>();
        // short message to give to the user
        public StringBuilder message = new StringBuilder();
        
        /** A new scheduler may be choose */    
        public boolean isNewSchedulerChoosen = false;
    }
    
    /**
     * Check is the scheduler is "go to go"
     * @param tenantId
     * @return
     */
    public abstract List<BEvent> check(long tenantId);

    public abstract StatusScheduler getStatus(long tenantId);

    public abstract List<BEvent> operation(Map<String, Serializable> parameters);

    /**
     * return the scheduler type
     */
    public enum TypeScheduler {
        QUARTZ, THREADSLEEP, PROCESS
    }

    public abstract TypeScheduler getType();

    /**
     * return the date of the next HeartBeat
     * 
     * @return
     */
    public abstract Date getDateNextHeartBeat(long tenantId);

    /**
     * info to give to the administrator
     * 
     * @return
     */
    public abstract String getDescription();

    /**
     * Deploy the scheduler
     * 
     * @param forceDeploy the deployment has to be redone
     * @param pageDirectory
     * @param tenantId
     * @param commandAPI
     * @param platFormAPI
     * @return
     */
    public abstract List<BEvent> checkAndDeploy(boolean forceDeploy, File pageDirectory, long tenantId);

    /**
     * reset the scheduler
     */
    public abstract List<BEvent> reset(long tenantId);

    
    /* ******************************************************************************** */
    /*                                                                                  */
    /* LogHeartBeat */
    /*                                                                                  */
    /* ******************************************************************************** */

    /**
     * Each Log Beart will be log as INFO
     */
    private boolean logHeartBeat = false;

    public boolean isLogHeartBeat() {
        return logHeartBeat;
    }
    public void setLogHeartBeat( boolean logHeartBeat) {
        this.logHeartBeat = logHeartBeat;
    }

    /**
     * Saved in database this number of heartBeat
     */
    private long nbSavedHeartBeat = 1;

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
}
