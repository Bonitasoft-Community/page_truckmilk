package org.bonitasoft.truckmilk.schedule;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bonitasoft.log.event.BEvent;

public interface MilkSchedulerInt {

    /**
     * initialize. Then the schedule must call back the engine on the method
     * milkCmdControl.executeOneTime( tenantId );
     * 
     * @param tenantId
     * @param forceReset
     * @return
     */
    public List<BEvent> startup(long tenantId, boolean forceReset);

    public List<BEvent> shutdown(long tenantId);

    /**
     * true if the scheduler is running (controled by start / stop
     * 
     * @return
     */
    public enum TypeStatus {
        STOPPED, SHUTDOWN, STARTED
    };

    public class StatusScheduler {

        public TypeStatus status;
        public List<BEvent> listEvents = new ArrayList<BEvent>();
    }

    public List<BEvent> check(long tenantId);

    public StatusScheduler getStatus(long tenantId);

    public List<BEvent> operation(Map<String, Serializable> parameters);

    /**
     * return the scheduler type
     */
    public enum TypeScheduler {
        QUARTZ, THREADSLEEP
    };

    public TypeScheduler getType();

    /**
     * return the date of the next HeartBeat
     * 
     * @return
     */
    public Date getDateNextHeartBeat(long tenantId);

    /**
     * info to give to the administrator
     * 
     * @return
     */
    public String getDescription();

    /**
     * Deploy the scheduler
     * 
     * @param forceDeploy the deployment has to be redone
     * @param pageDirectory
     * @param commandAPI
     * @param platFormAPI
     * @param tenantId
     * @return
     */
    public List<BEvent> checkAndDeploy(boolean forceDeploy, File pageDirectory, long tenantId);

    /**
     * reset the scheduler
     */
    public List<BEvent> reset(long tenantId);

}
