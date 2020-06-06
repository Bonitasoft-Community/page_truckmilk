package org.bonitasoft.truckmilk.plugin;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobContext;
/* ******************************************************************************** */
/*                                                                                  */
/* Ping */
/*                                                                                  */
/* this class may be use as a skeleton for a new plug in */
/* Attention, reference any new plug in in MilPlugInFactory.collectListPlugIn() */
/*                                                                                  */
/* ******************************************************************************** */
import org.bonitasoft.truckmilk.job.MilkJobExecution;

public class MilkPing extends MilkPlugIn {

    private final static PlugInParameter cstParamAddDate = PlugInParameter.createInstance("addDate", "Add a date", TypeParameter.BOOLEAN, true, "If set, the date of execution is added in the status of execution");
    private final static PlugInParameter cstParamTimeExecution = PlugInParameter.createInstance("timeExecution", "Time execution (in mn)", TypeParameter.LONG, true, "The job will run this time, and will update the % of execution each minutes");

    private final static PlugInMeasurement cstMesureMS         = PlugInMeasurement.createInstance( "MS", "Random Value", "Give a random value to demonstrate the function");
    private final static PlugInMeasurement cstMesureHourOfDay  = PlugInMeasurement.createInstance( "HourOfDay", "Hour of day", "Register the hour of day when the execution ran");
    
    private final static String LOGGER_LABEL = "MilkPing";
    private final static Logger logger = Logger.getLogger(MilkPing.class.getName());

    private final static BEvent eventPing = new BEvent(MilkPing.class.getName(), 1, Level.INFO,
            "Ping !", "The ping job is executed correctly");

    private final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

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
    public List<BEvent> checkPluginEnvironment(MilkJobExecution milkJobExecution) {
        return new ArrayList<>();
    }

    /**
     * check the Job's environment
     */
    public List<BEvent> checkJobEnvironment(MilkJobExecution milkJobExecution) {
        return new ArrayList<>();
    }

    /**
     * return the description of ping job
     */
    @Override
    public MilkPlugInDescription getDefinitionDescription(MilkJobContext milkJobContext) {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();

        plugInDescription.setName( "Ping" );
        plugInDescription.setExplanation( "Just do a ping" );
        plugInDescription.setLabel( "Ping job" );
        plugInDescription.setStopJob( JOBSTOPPER.MAXMINUTES);
        
        plugInDescription.addParameter(cstParamAddDate);
        plugInDescription.addParameter(cstParamTimeExecution);
        
        plugInDescription.addMesure( cstMesureMS );
        plugInDescription.addMesure( cstMesureHourOfDay);
        return plugInDescription;
    }

    /**
     * execution of the job. Just calculated the result according the parameters, and return it.
     */
    @Override
    public MilkJobOutput execute(MilkJobExecution jobExecution) {
        MilkJobOutput plugTourOutput = jobExecution.getMilkJobOutput();

        // if the date has to be added in the result ?
        Boolean addDate = jobExecution.getInputBooleanParameter(cstParamAddDate.name, Boolean.TRUE);
        Long totalMinutesExecution = jobExecution.getInputLongParameter(cstParamTimeExecution.name, 1L);
        if (totalMinutesExecution > 60)
            totalMinutesExecution = 60L;

        long total10Step = totalMinutesExecution * 6;
        String parameters = Boolean.TRUE.equals(addDate) ? "Date: " + sdf.format(new Date()) : "";

        jobExecution.setAvancementTotalStep(total10Step);

        for (long step10s = 0; step10s < total10Step; step10s++) {
            if (jobExecution.pleaseStop())
                break;
            jobExecution.setAvancementStep(step10s);
            try {
                Thread.sleep( 1000L * 10);
            } catch (InterruptedException e) {
                logger.severe(LOGGER_LABEL+"Interrupted !"+ e.toString());
                Thread.currentThread().interrupt();
            }
        }
        long valuePoint = (System.currentTimeMillis() % 1000) - 250;
        if (valuePoint<250)
            valuePoint=0;
        plugTourOutput.setMeasure( cstMesureMS, valuePoint);
        
        Calendar c = Calendar.getInstance();
        plugTourOutput.setMeasure( cstMesureHourOfDay, c.get( Calendar.HOUR_OF_DAY));
        
        jobExecution.setAvancementStep(totalMinutesExecution);

        plugTourOutput.addEvent(new BEvent(eventPing, parameters));
        plugTourOutput.executionStatus = ExecutionStatus.SUCCESS;
        if (jobExecution.pleaseStop())
            plugTourOutput.executionStatus = ExecutionStatus.SUCCESSPARTIAL;

        return plugTourOutput;
    }

}
