package org.bonitasoft.truckmilk.engine;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfo;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfoSearchDescriptor;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.DELAYSCOPE;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.TypesCast;



public class MilkPlugInToolbox {
    
    private static BEvent eventNoProcessByTheFilter = new BEvent(MilkPlugInToolbox.class.getName(), 1,
            Level.APPLICATIONERROR,
            "No enable process by the filter", "The process filter does not match any enable process", "Jobs may have an empty result.",
            "Give a enable process name");
    private static BEvent eventNoProcessesListFilter = new BEvent(MilkPlugInToolbox.class.getName(), 2,
            Level.APPLICATIONERROR,
            "List of processes is empty", "List of process is empty, one filter should be give minimum", "Job has to result.",
            "Give one filter minimum");
    private static BEvent eventIncorrectDelayStructure = new BEvent(MilkPlugInToolbox.class.getName(), 3,
            Level.ERROR,
            "Incorrect Delay Structure", "Delay structure expected is SCOPE:VALUE", "No delay can be calculated",
            "Check error");


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
    public static ListProcessesResult completeListProcess(MilkJobExecution jobExecution, 
            PlugInParameter parameter,
            boolean expectOneProcessMinimum,
            SearchOptionsBuilder sob, 
            String searchAttributName, 
            ProcessAPI processAPI) throws SearchException {
        ListProcessesResult processParametersResult = new ListProcessesResult();
        
        if (TypeParameter.PROCESSNAME.equals(parameter.typeParameter) ) {
            processParametersResult.listProcessNameVersion = new ArrayList<>();
            processParametersResult.listProcessNameVersion.add(  jobExecution.getInputStringParameter( parameter ));
        }            
        else {            
            processParametersResult.listProcessNameVersion = (List<String>) jobExecution.getInputListParameter(parameter);
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
                SearchResult<ProcessDeploymentInfo> searchResult = getListProcessDefinitionId(processNameVersion, processAPI);
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
    /**
     * when the parameter is a TypeParameter.PROCESSNAME
     * @param processNameVersion
     * @param processAPI
     * @return
     * @throws SearchException
     */
    public static SearchResult<ProcessDeploymentInfo> getListProcessDefinitionId(String processNameVersion, ProcessAPI processAPI) throws SearchException {
        if (processNameVersion==null)
            processNameVersion="";
        processNameVersion = processNameVersion.trim();
        String processNameOnly = processNameVersion;
        String processVersionOnly = null;
        if (processNameVersion.endsWith(")")) {
            int firstParenthesis = processNameVersion.lastIndexOf('(');
            if (firstParenthesis != -1) {
                processNameOnly = processNameVersion.substring(0, firstParenthesis);
                processVersionOnly = processNameVersion.substring(firstParenthesis + 1, processNameVersion.length() - 1);
            }
        }
        SearchOptionsBuilder searchOption = new SearchOptionsBuilder(0, 1000);
        searchOption.filter(ProcessDeploymentInfoSearchDescriptor.NAME, processNameOnly.trim());
        if (processVersionOnly != null) {
            searchOption.filter(ProcessDeploymentInfoSearchDescriptor.VERSION, processVersionOnly.trim());
        }
        searchOption.filter(ProcessDeploymentInfoSearchDescriptor.ACTIVATION_STATE, "ENABLED");

        return processAPI.searchProcessDeploymentInfos(searchOption.done());

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
    public static DelayResult getTimeFromDelay(MilkJobExecution jobExecution, PlugInParameter parameter,Date currentDate, boolean advance) {
        return getTimeFromDelay(jobExecution.getInputMapParameter(parameter), TypesCast.getString( parameter.defaultValue, "" ), currentDate, advance);
    }
            
    public static DelayResult getTimeFromDelay( Map<String,Object> mapScope, String defaultValue, Date currentDate, boolean advance) {
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
    
    /* ******************************************************************************** */
    /*                                                                                  */
    /* Create Case, Execute Tasks */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    public List<BEvent> createCase() {
        return new ArrayList<>();
    }

    public List<BEvent> executeTasks() {
        return new ArrayList<>();
    }

}
