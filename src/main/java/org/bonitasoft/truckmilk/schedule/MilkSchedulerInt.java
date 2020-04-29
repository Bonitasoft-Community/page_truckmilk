package org.bonitasoft.truckmilk.schedule;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.truckmilk.toolbox.MilkLog;
import org.hibernate.bytecode.internal.javassist.FastClass;

import lombok.Data;

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
        STOPPED, SHUTDOWN, STARTED
    };

    public static class StatusScheduler {

        public TypeStatus status;
        public List<BEvent> listEvents = new ArrayList<>();
    }

    public abstract List<BEvent> check(long tenantId);

    public abstract StatusScheduler getStatus(long tenantId);

    public abstract List<BEvent> operation(Map<String, Serializable> parameters);

    /**
     * return the scheduler type
     */
    public enum TypeScheduler {
        QUARTZ, THREADSLEEP, PROCESS
    };

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

   
}
