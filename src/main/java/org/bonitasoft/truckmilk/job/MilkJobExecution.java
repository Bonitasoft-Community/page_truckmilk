package org.bonitasoft.truckmilk.job;

import java.io.OutputStream;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.service.TenantServiceAccessor;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox.DelayResult;
import org.bonitasoft.truckmilk.engine.MilkSerializeProperties.SerializationJobParameters;
import org.bonitasoft.truckmilk.toolbox.MilkLog;
import org.bonitasoft.truckmilk.toolbox.TypesCast;

/* ******************************************************************************** */
/*                                                                                  */
/* Execution */
/*                                                                                  */
/*                                                                                  */
/* ******************************************************************************** */

public class MilkJobExecution {

    static MilkLog logger = MilkLog.getLogger(MilkJobExecution.class.getName());

    // this parameters is initialised with the value in the PlugTour, and may change after
    private Map<String, Object> jobParameters;

    private MilkJob milkJob;

    /**
     * tenant where the job is running
     */
    MilkJobContext milkJobContext;
    
    public MilkJobExecution(MilkJobContext milkJobContext) {
        this.milkJob = null;
        this.jobParameters = null;
        this.milkJobContext = milkJobContext;
    }

    public MilkJobExecution(MilkJob milkJob, MilkJobContext milkJobContext) {
        this.milkJob = milkJob;
        this.jobParameters = milkJob.getJobParameters();

        this.milkJobContext = milkJobContext;
 
        if ( milkJob.getPlugIn().getDescription().isJobCanBeStopByMaxMinutes()) {
            if (milkJob.getStopAfterMaxMinutes() > 0)
                pleaseStopAfterMaxMinutes = milkJob.getStopAfterMaxMinutes();
            else if (milkJob.getPlugIn().getDescription().getJobStopMaxTimeMn() != null)
                pleaseStopAfterMaxMinutes =milkJob.getPlugIn().getDescription().getJobStopMaxTimeMn();
            else 
                pleaseStopAfterMaxMinutes = MilkJob.CSTDEFAULT_STOPAFTER_MAXMINUTES;
        }
        
        if ( milkJob.getPlugIn().getDescription().isJobCanBeStopByMaxItems()) {
            if (milkJob.getStopAfterMaxItems()>0)
                pleaseStopAfterMaxItems = milkJob.getStopAfterMaxItems();
            else if (milkJob.getPlugIn().getDescription().getJobStopMaxItems() != null)
                pleaseStopAfterMaxItems = milkJob.getPlugIn().getDescription().getJobStopMaxItems();
            else
                pleaseStopAfterMaxItems = MilkJob.CSTDEFAULT_STOPAFTER_MAXITEMS;
        }
    }
    
    public MilkJobOutput getMilkJobOutput() {
        return new MilkJobOutput(milkJob);
    }

    
    public MilkJob getMilkJob() {
        return milkJob;
    }
    /**
     * Boolean
     * 
     * @param parameter
     * @return
     */
    public Boolean getInputBooleanParameter(PlugInParameter parameter) {
        return getInputBooleanParameter(parameter.name, (Boolean) parameter.defaultValue);
    }

    public Boolean getInputBooleanParameter(String name, Boolean defaultValue) {
        return TypesCast.getBoolean(jobParameters.get(name) ==null ? defaultValue : jobParameters.get(name).toString(), defaultValue);

    }

    /**
     * Long
     * 
     * @param parameter
     * @return
     */
    public Long getInputLongParameter(PlugInParameter parameter) {
        return getInputLongParameter(parameter.name, parameter.defaultValue == null ? null : Long.valueOf(parameter.defaultValue.toString()));
    }

    public Long getInputLongParameter(String name, Long defaultValue) {
        return TypesCast.getLong(jobParameters.get(name), defaultValue);
    }

    public String getInputStringParameter(PlugInParameter parameter) {
        return getInputStringParameter(parameter.name, (String) parameter.defaultValue);
    }

    public String getInputStringParameter(String name, String defaultValue) {
        return TypesCast.getString(jobParameters.get(name), defaultValue);
    }

    public DelayResult getInputDelayParameter(PlugInParameter parameter, Date baseDate, boolean advance) {
        return MilkPlugInToolbox.getTimeFromDelay(this, parameter, baseDate, advance);
    }
    
    
    @SuppressWarnings("unchecked")
    public Map<String,Object> getInputMapParameter(PlugInParameter parameter) {
        return (Map<String,Object>) jobParameters.get(parameter.name);

    }

    
    public void getStreamParameter(PlugInParameter plugInParameter, OutputStream output) {

        if (plugInParameter.typeParameter == TypeParameter.FILEREAD || plugInParameter.typeParameter == TypeParameter.FILEREADWRITE || plugInParameter.typeParameter == TypeParameter.FILEWRITE) {
            // update the PLUGIN parameters
            milkJob.getParameterStream(plugInParameter, output);
        } else {
            logger.severe("setParameterStream not allowed on parameter[" + plugInParameter.name + "] (plugin " + milkJob.getPlugIn().getName() + "]");
        }
    }

    public List<?> getInputListParameter(PlugInParameter parameter) {
        return getInputListParameter(parameter.name, (List<?>) parameter.defaultValue);
    }

    public List<?> getInputListParameter(String name, List<?> defaultValue) {
        try {
            return (List<?>) jobParameters.get(name);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> getInputListMapParameter(PlugInParameter parameter) {
        return (List<Map<String, Object>>) getInputListParameter(parameter.name, (List<Map<String, Object>>) parameter.defaultValue);
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> getInputListMapParameter(String name, List<Map<String, Object>> defaultValue) {
        try {
            return (List<Map<String, Object>>) jobParameters.get(name);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * getter 
     * 
     */
    public MilkJobContext getMilkJobContext() {
        return milkJobContext;
    }
    public long getTenantId() {
        return milkJobContext.getTenantId();
    }
    public APIAccessor getApiAccessor() {
        return milkJobContext.getApiAccessor();
    }
    
    public TenantServiceAccessor getTenantServiceAccessor() {
        return milkJobContext.getTenantServiceAccessor();
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* please Stop mechanism */
    /*                                                                                  */
    /* The please stop mechanism can be setup by a maximum time, maximum item process */
    /* or via the interface by an adminitrator. Plugin are suppose to check the */
    /*
     * pleaseStop() method
     * /*
     */
    /* ******************************************************************************** */
    private Integer pleaseStopAfterMaxMinutes = null;
    private Integer pleaseStopAfterMaxItems = null;
    private int nbManagedItems = 0;
    private int nbPrepareditems = 0;


   
    /**
     * in advancement, there are 2 concepts:
     * - the number of step advance
     * - the number of item managed
     * Example : you have a list of cases potentialy to delete in a list of 2000. Not all lines will have a case to delete.
     * If you setup the number of managedItem to 400, you want to stop after 400 deletions.
     * 
     * @param nbStepManagedItem
     */

    public boolean isLimitationOnMaxItem() {
        return (pleaseStopAfterMaxItems != null && pleaseStopAfterMaxItems>0);
    }
    public Integer getJobStopAfterMaxItems() {
        return pleaseStopAfterMaxItems;
    }
    
    public boolean isLimitationOnMaxMinutes() {
        return pleaseStopAfterMaxMinutes != null && pleaseStopAfterMaxMinutes>0;
    }
    
    public Integer getJobStopAfterMaxMinutes() {
        return pleaseStopAfterMaxMinutes;
    }
    
    public int getManagedItems() {
        return this.nbManagedItems;
    }
    public void addManagedItems(long managedItems) {
        this.nbManagedItems += managedItems;
    }

    /**
     * Some items may be ready to be executed, but not yet executed : there are in preparation.
     * they are not yet register in the nbManagedItems. This number is take into account in the pleaseStop calculation.
     * When the pleaseStop is based on a number of Item (example, 40), and 35 are already managed , but 5 are in preparation, the pleaseStop() will return true
     * then.
     * ATTENTION: it's the caller responsability to update this number, set back to 0 when the prepared item are managed.
     * 
     * @param itemInPreparation : Number of Item in preparation
     */
    public void setPreparationItems(int nbPrepareditems) {
        this.nbPrepareditems = nbPrepareditems;
    }
    public void addPreparationItems(int nbPrepareditems) {
        this.nbPrepareditems += nbPrepareditems;
    }

    boolean pleaseStop = false;

    public void setPleaseStop(boolean pleaseStop) {
        this.pleaseStop = pleaseStop;
    }
    /**
     * PleaseStop
     * 
     * @return
     */
    public boolean pleaseStop() {
        if (pleaseStop)
            return true;        
        if (milkJob.isAskedForStop())
            return true;
        if (isLimitationOnMaxMinutes()) {
            long currentTime = System.currentTimeMillis();
            long timeInMn = (currentTime - startTimeMs) / 1000 / 60;
            if (timeInMn > pleaseStopAfterMaxMinutes) {
                return true;
            }
        }
        if (isLimitationOnMaxItem()) {
            if (nbManagedItems + nbPrepareditems >= pleaseStopAfterMaxItems) {
                return true;
            }
        }
        // no reason to stop
        return false;

    }

    public String getStopExplanation() {
        if (pleaseStop)
            return "Manual stop";        
        if (milkJob.isAskedForStop())
            return "Administrator Manual stop";        
        if (pleaseStopAfterMaxMinutes != null && pleaseStopAfterMaxMinutes>0) {
            long currentTime = System.currentTimeMillis();
            long timeInMn = (currentTime - startTimeMs) / 1000 / 60;
            if (timeInMn > pleaseStopAfterMaxMinutes) {
                return "Too long : TimeInMn["+timeInMn+"], MaxTimeAllowed=["+pleaseStopAfterMaxMinutes+"]";
            }
        }
        if (pleaseStopAfterMaxItems != null && pleaseStopAfterMaxItems>0) {
            if (nbManagedItems + nbPrepareditems >= pleaseStopAfterMaxItems) {
                return "Too long : Items["+(nbManagedItems + nbPrepareditems)+"], MaxItemsAllowed=["+pleaseStopAfterMaxItems+"]";
            }
        } 
        return "";
    }
    /* ******************************************************************************** */
    /*                                                                                  */
    /* Advancement update */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    public long totalStep;

    /**
     * Plug in can set an % of advancement if it want
     * 
     * @param advancementInPercent
     */
    public void setAvancementTotalStep(long totalStep) {
        this.totalStep = totalStep;
        // register in the tour the % of advancement, and then calculated an estimated end date.
    }

    /**
     * Plug in can set an step. Attention, it's not an ADD, but to set the step where works are (so, you can come back if you wants)
     * 
     * @param advancementInPercent
     */
    public void setAvancementStep(long step) {
        if (totalStep != 0)
            setAvancement((int) ((100 * step) / totalStep));
        // register in the tour the % of advancement, and then calculated an estimated end date.
    }

    /**
     * /**
     * Plug in can set an % of advancement if it want
     * 
     * @param advancementInPercent
     */
    public void setAvancement(int advancementInPercent) {
        if (advancementInPercent != milkJob.getPercent()) {
            // update
            milkJob.setPercent( advancementInPercent );
            long timeExecution = System.currentTimeMillis() - startTimeMs;
            if (timeExecution > 0 && advancementInPercent > 0) {
                milkJob.setTotalTimeEstimatedInMs ( timeExecution * 100 / advancementInPercent);
                milkJob.setEndTimeEstimatedInMs( milkJob.getTotalTimeEstimatedInMs() - timeExecution );
                milkJob.setEndDateEstimated( new Date(milkJob.getEndTimeEstimatedInMs() + startTimeMs));
                // save the current advancement
                SerializationJobParameters saveParameters = SerializationJobParameters.getInstanceTrackExecution();
                milkJob.milkJobFactory.dbSaveJob(milkJob, saveParameters);
            }
        }
        // register in the tour the % of advancement, and then calculated an estimated end date.
    }

    private long startTimeMs;

    /**
     * plug in can overwrite this method, but please call this
     */
    public void start() {
        startTimeMs = System.currentTimeMillis();
        pleaseStop = false;

        milkJob.setImmediateExecution( false );
        milkJob.setInExecution( true );
        milkJob.setStartTime( startTimeMs );
        milkJob.setPercent( 0 );
        milkJob.setEndTimeEstimatedInMs( -1 );
        milkJob.setEndDateEstimated( null );

    }

    public void end() {
        milkJob.setInExecution( false );
        milkJob.setPercent( 100 );
        milkJob.setEndTimeEstimatedInMs( 0 );
        milkJob.setEndDateEstimated( new Date() );

    }

    public long getTimeFromStartupInMs() {
        return System.currentTimeMillis() - startTimeMs;
    }
}
