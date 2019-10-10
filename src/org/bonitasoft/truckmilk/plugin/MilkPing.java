package org.bonitasoft.truckmilk.plugin;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.job.MilkJobExecution;

/* ******************************************************************************** */
/*                                                                                  */
/* Ping */
/*                                                                                  */
/* this class may be use as a skeleton for a new plug in */
/*                                                                                  */
/* ******************************************************************************** */

public class MilkPing extends MilkPlugIn {

    private static PlugInParameter cstParamAddDate = PlugInParameter.createInstance("addDate", "Add a date", TypeParameter.BOOLEAN, true, "If set, the date of execution is added in the status of execution");
    private static PlugInParameter cstParamTimeExecution = PlugInParameter.createInstance("timeExecution", "Time execution (in mn)", TypeParameter.LONG, true, "The job will run this time, and will update the % of execution each minutes");

    private static BEvent eventPing = new BEvent(MilkPing.class.getName(), 1, Level.INFO,
            "Ping !", "The ping job is executed correctly");

    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

    public MilkPing() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * plug in can check its environment, to detect if you missed something. An external component may
     * be required and are not installed.
     * 
     * @return a list of Events.
     */
    @Override
    public List<BEvent> checkEnvironment(long tenantId, APIAccessor apiAccessor) {
        return new ArrayList<BEvent>();
    }

    /**
     * return the description of ping job
     */
    @Override
    public PlugInDescription getDefinitionDescription() {
        PlugInDescription plugInDescription = new PlugInDescription();

        plugInDescription.name = "Ping";
        plugInDescription.description = "Just do a ping";
        plugInDescription.label = "Ping job";
        plugInDescription.addParameter(cstParamAddDate);
        plugInDescription.addParameter(cstParamTimeExecution);
        return plugInDescription;
    }

    /**
     * execution of the job. Just calculated the result according the parameters, and return it.
     */
    @Override
    public PlugTourOutput execute(MilkJobExecution jobExecution, APIAccessor apiAccessor) {
        PlugTourOutput plugTourOutput = jobExecution.getPlugTourOutput();

        // if the date has to be added in the result ?
        Boolean addDate = jobExecution.getInputBooleanParameter(cstParamAddDate.name, Boolean.TRUE);
        Long totalMinutesExecution = jobExecution.getInputLongParameter(cstParamTimeExecution.name, 1L);
        if (totalMinutesExecution>60)
            totalMinutesExecution=60L;
        
        long total10Step = totalMinutesExecution*6;
        String parameters = addDate ? "Date: " + sdf.format(new Date()) : "";
        
        jobExecution.setAvancementTotalStep(total10Step);
        
        for (long step10s =0; step10s < total10Step;step10s ++)
        {
            if (jobExecution.pleaseStop())
                break;
            jobExecution.setAvancementStep( step10s );
            try {
                Thread.sleep(1000*10);
            } catch (InterruptedException e) {
               
            }
        }
        jobExecution.setAvancementStep( totalMinutesExecution);
        
        
        plugTourOutput.addEvent(new BEvent(eventPing, parameters));
        plugTourOutput.executionStatus = ExecutionStatus.SUCCESS;
        if (jobExecution.pleaseStop())
            plugTourOutput.executionStatus = ExecutionStatus.SUCCESSPARTIAL;

        return plugTourOutput;
    }

}
