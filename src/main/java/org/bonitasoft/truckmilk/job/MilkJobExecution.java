package org.bonitasoft.truckmilk.job;

import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfo;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.engine.service.TenantServiceAccessor;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkConstantJson;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.DELAYSCOPE;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.engine.MilkSerializeProperties.SerializationJobParameters;
import org.bonitasoft.truckmilk.job.MilkJob.SavedExecution;
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

    private static BEvent eventNoProcessByTheFilter = new BEvent(MilkJobExecution.class.getName(), 1,
            Level.APPLICATIONERROR,
            "No enable process by the filter", "The process filter does not match any enable process", "Jobs may have an empty result.",
            "Give a enable process name");
    private static BEvent eventNoProcessesListFilter = new BEvent(MilkJobExecution.class.getName(), 2,
            Level.APPLICATIONERROR,
            "List of processes is empty", "List of process is empty, one filter should be give minimum", "Job has to result.",
            "Give one filter minimum");
    private static BEvent eventIncorrectDelayStructure = new BEvent(MilkJobExecution.class.getName(), 3,
            Level.ERROR,
            "Incorrect Delay Structure", "Delay structure expected is SCOPE:VALUE", "No delay can be calculated",
            "Check error");

    
    
    // this parameters is initialised with the value in the PlugTour, and may change after
    private Map<String, Object> jobParameters;

    private MilkJob milkJob;

    /**
     * tenant where the job is running
     */
    MilkJobContext milkJobContext;
    
    MilkJobOutput milkJobOuptput;
    
    
    
    public MilkJobExecution(MilkJobContext milkJobContext) {
        this.milkJob = null;
        this.jobParameters = null;
        this.milkJobContext = milkJobContext;
        this.milkJobOuptput =  new MilkJobOutput(milkJob);
    }

    public MilkJobExecution(MilkJob milkJob, MilkJobContext milkJobContext) {
        this.milkJob = milkJob;
        this.jobParameters = milkJob.getJobParameters();
        this.milkJobContext = milkJobContext;
        this.milkJobOuptput =  new MilkJobOutput(milkJob);

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
        return milkJobOuptput;
    }

    
    public MilkJob getMilkJob() {
        return milkJob;
    }
    
    

/* ******************************************************************************** */
/*                                                                                  */
/* getInputParameter */
/*                                                                                  */
/*                                                                                  */
/* ******************************************************************************** */

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

    
    /**
     * Delay access
     * @param parameter
     * @param baseDate
     * @param advance
     * @return
     */
    public DelayResult getInputDelayParameter(PlugInParameter parameter, Date baseDate, boolean advance) {
        // return getTimeFromDelay(this, parameter, baseDate, advance);
       return getTimeFromDelay(getInputMapParameter(parameter), TypesCast.getString( parameter.defaultValue, "" ), baseDate, advance);
    }
    
    public static class DelayResult {
        public List<BEvent> listEvents= new ArrayList<>();
        public DELAYSCOPE scopeInput;
        public int delayInput;
        public Date originDate;
        public Date delayDate;
        /**
         * positif if advance = true, negatif else (if delay is in the past)
         */
        public long delayInMs;
    }
   /* public static DelayResult getTimeFromDelay(MilkJobExecution jobExecution, PlugInParameter parameter,Date currentDate, boolean advance) {
        return getTimeFromDelay(jobExecution.getInputMapParameter(parameter), TypesCast.getString( parameter.defaultValue, "" ), currentDate, advance);
    }
      */      
    protected static DelayResult getTimeFromDelay( Map<String,Object> mapScope, String defaultValue, Date currentDate, boolean advance) {
        DelayResult delayResult = new DelayResult();
        delayResult.originDate = currentDate;
        
        // mapScope is given? Use it
        try
        {
            if (mapScope != null) {
                delayResult.scopeInput = DELAYSCOPE.valueOf( mapScope.get( MilkConstantJson.CSTJSON_PARAMETER_DELAY_SCOPE).toString());
                delayResult.delayInput = Integer.parseInt(mapScope.get( MilkConstantJson.CSTJSON_PARAMETER_DELAY_VALUE).toString());
            } else {
                StringTokenizer st = new StringTokenizer(defaultValue, ":");
                delayResult.scopeInput = DELAYSCOPE.valueOf( st.nextToken());
                delayResult.delayInput = Integer.parseInt( st.nextToken());
            }
            if (! advance)
                delayResult.delayInput = - delayResult.delayInput;
        }
        catch( Exception e)
        {
            if (delayResult.scopeInput == null)
                delayResult.scopeInput= DELAYSCOPE.YEAR;
            delayResult.listEvents.add( new BEvent(eventIncorrectDelayStructure, "Value:["+defaultValue+"]"));
            return delayResult;
        }
        
        Calendar c = Calendar.getInstance();
        c.setTime(currentDate);
        if (delayResult.scopeInput == DELAYSCOPE.YEAR)
            c.add(Calendar.YEAR, delayResult.delayInput);
        else if (delayResult.scopeInput == DELAYSCOPE.MONTH)
            c.add(Calendar.MONTH, delayResult.delayInput);
        else if (delayResult.scopeInput == DELAYSCOPE.WEEK)
            c.add(Calendar.WEEK_OF_YEAR, delayResult.delayInput);
        else if (delayResult.scopeInput == DELAYSCOPE.DAY)
            c.add(Calendar.DAY_OF_YEAR, delayResult.delayInput);
        else if (delayResult.scopeInput == DELAYSCOPE.HOUR)
            c.add(Calendar.HOUR_OF_DAY, delayResult.delayInput);
        else if (delayResult.scopeInput == DELAYSCOPE.MN)
            c.add(Calendar.MINUTE, delayResult.delayInput);
        
        delayResult.delayDate = c.getTime();
        delayResult.delayInMs = c.getTimeInMillis() - delayResult.originDate.getTime();
        return delayResult;
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
     * Tool for plugin : from a TypeParameter.PROCESSNAME or TypeParameter.ARRAYPROCESSNAME, return the list of ProcessDefintionId and complete a SearchOptionBuilder
     * @author Firstname Lastname
     *
     */
    public static class ListProcessesResult {
        public List<BEvent> listEvents= new ArrayList<>();
        public List<String> listProcessNameVersion;
        public List<ProcessDeploymentInfo> listProcessDeploymentInfo = new ArrayList<>();
        public SearchOptionsBuilder sob;
    }
    @SuppressWarnings("unchecked")
    public ListProcessesResult getInputArrayProcess( PlugInParameter parameter,
            boolean expectOneProcessMinimum,
            SearchOptionsBuilder sob, 
            String searchAttributName, 
            ProcessAPI processAPI) throws SearchException {
        ListProcessesResult processParametersResult = new ListProcessesResult();
        
        if (TypeParameter.PROCESSNAME.equals(parameter.typeParameter) ) {
            processParametersResult.listProcessNameVersion = new ArrayList<>();
            processParametersResult.listProcessNameVersion.add(  getInputStringParameter( parameter ));
        }            
        else {            
            processParametersResult.listProcessNameVersion = (List<String>) getInputListParameter(parameter);
            if (processParametersResult.listProcessNameVersion == null)
                processParametersResult.listProcessNameVersion = new ArrayList<>();
        }

        processParametersResult.sob= sob ==null ? new SearchOptionsBuilder(0,10000): sob;
        boolean oneProcessFound=false;
        if (! processParametersResult.listProcessNameVersion.isEmpty())
        {
            int count=0;
            for (int i=0;i<processParametersResult.listProcessNameVersion.size();i++) {
                  
                String processNameVersion = processParametersResult.listProcessNameVersion.get( i );
                SearchResult<ProcessDeploymentInfo> searchResult = MilkPlugInToolbox.getListProcessDefinitionId(processNameVersion,  parameter.filterProcess, processAPI);
                if (searchResult.getCount()==0)
                {
                    processParametersResult.listEvents.add( new BEvent(eventNoProcessByTheFilter, "ProcessFilter["+processNameVersion+"]"));
                }
                for (ProcessDeploymentInfo processDeployment : searchResult.getResult())
                {
                    if (! oneProcessFound) {
                        oneProcessFound=true;
                        processParametersResult.sob.leftParenthesis();
                    }

                    if (count>0)
                        processParametersResult.sob.or();
                    count++;
                    processParametersResult.sob.filter( searchAttributName, processDeployment.getProcessId() );
                    processParametersResult.listProcessDeploymentInfo.add( processDeployment );
                }
            }
            if (oneProcessFound) {
                processParametersResult.sob.rightParenthesis();
            }
        }
        if (expectOneProcessMinimum && processParametersResult.listProcessDeploymentInfo.isEmpty()) {
            processParametersResult.listEvents.add( eventNoProcessesListFilter);
        }

        
        return processParametersResult;
    }
    
   
/* ******************************************************************************** */
/*                                                                                  */
/* Getter */
/*                                                                                  */
/*                                                                                  */
/* ******************************************************************************** */

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
    public boolean isStopRequired() {
        if (pleaseStop)
            return true;        
        if (milkJob.isAskedForStop())
            return true;
        if (isLimitationOnMaxMinutes()) {
            long currentTime = System.currentTimeMillis();
            long timeInMn = (currentTime - milkJob.getTrackExecution().startTime) / 1000 / 60;
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
            long timeInMn = (currentTime - milkJob.getTrackExecution().startTime) / 1000 / 60;
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
            long timeExecution = System.currentTimeMillis() - milkJob.getTrackExecution().startTime;
            if (timeExecution > 0 && advancementInPercent > 0) {
                milkJob.setTotalTimeEstimatedInMs ( timeExecution * 100 / advancementInPercent);
                milkJob.setEndTimeEstimatedInMs( milkJob.getTotalTimeEstimatedInMs() - timeExecution );
                milkJob.setEndDateEstimated( new Date(milkJob.getEndTimeEstimatedInMs() + milkJob.getTrackExecution().startTime));
                saveAdvancement( false );
            }
        }
        // register in the tour the % of advancement, and then calculated an estimated end date.
    }
    /**
     * Job can give an information on what's doing. There is no history here.
     * @param information
     */
    public void setAvancementInformation(String information) {
        SimpleDateFormat sdfSynthetic = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        milkJob.setAvancementInformation( sdfSynthetic.format( new Date())+" "+ information );
        saveAdvancement( false ); // avoid too much update
    }
    
    private long lastTimeSaved=0;

    private void saveAdvancement(boolean force) {
        long currentTime = System.currentTimeMillis();
        if (lastTimeSaved==0 || currentTime-lastTimeSaved>60*1000 || force)
        {
            // save the current advancement
            SavedExecution savedExecution = milkJob.getCurrentExecution();
            if (savedExecution!=null)
                savedExecution.reportInHtml = milkJobOuptput.getReportInHtml();
            
            SerializationJobParameters saveParameters = SerializationJobParameters.getInstanceTrackExecution();
            milkJob.milkJobFactory.dbSaveJob(milkJob, saveParameters);
            lastTimeSaved = currentTime;
        }
    }
    

    /**
     * plug in can overwrite this method, but please call this
     */
    public void start() {
        pleaseStop = false;

        milkJob.setImmediateExecution( false );
        milkJob.setInExecution( true );
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
        return System.currentTimeMillis() - milkJob.getTrackExecution().startTime;
    }
}
