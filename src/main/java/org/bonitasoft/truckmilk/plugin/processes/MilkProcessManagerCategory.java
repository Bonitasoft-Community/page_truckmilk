package org.bonitasoft.truckmilk.plugin.processes;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.process.ProcessDefinition;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfo;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfoSearchDescriptor;
import org.bonitasoft.engine.search.Order;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.store.BonitaStoreAccessor;
import org.bonitasoft.store.artifactdeploy.DeployStrategyProcess;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

public class MilkProcessManagerCategory extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkProcessManagerCategory.class.getName());
    private final static String LOGGER_LABEL = "MilkProcessManagerCategory ";

   
    private final static BEvent eventSearchFailed = new BEvent(MilkDeleteProcesses.class.getName(), 1, Level.ERROR,
            "Search failed", "Search failed task return an error", "No retry can be performed", "Check the error");

    private final static String CSTCATEGORYPOLICY_CREATEIFMISSING = "Create the category if missing";
    private final static String CSTCATEGORYPOLICY_DONOTHING = "Do not creates the category";


    private static PlugInParameter cstParamProcessManagerEnable = PlugInParameter.createInstance("ProcessManager", "Process Managment", TypeParameter.BOOLEAN, Boolean.TRUE, 
            "Enable the Process manager policy. An actor named "+DeployStrategyProcess.CST_PROCESS_MANAGER+" is used to generate the process Manager mapping");
    private static PlugInParameter cstParamCategoryEnable = PlugInParameter.createInstance("Category", "Category", TypeParameter.BOOLEAN, Boolean.TRUE, 
            "Enable the Category affectation. Description is read, and if a line started by "+DeployStrategyProcess.CST_CATEGORY+", then the list of category is used to register the process in the given category. Categories may be a list : review,quality,production");
    private static PlugInParameter cstParamCategoryPolicy = PlugInParameter.createInstanceListValues("categoryPolicy", "Categories Policy",
            new String[] { CSTCATEGORYPOLICY_CREATEIFMISSING, CSTCATEGORYPOLICY_DONOTHING }, CSTCATEGORYPOLICY_CREATEIFMISSING,
            "Purge cases policy. If the process is called as a sub process, sub case is not purged")
            .withVisibleConditionParameterValueEqual(cstParamCategoryEnable, true);
    
        public MilkProcessManagerCategory() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * check the environment : for the deleteCase, nothing is required
     */
    public List<BEvent> checkPluginEnvironment(MilkJobExecution milkJobExecution) {
        return new ArrayList<>();
    }

    @Override
    public List<BEvent> checkJobEnvironment(MilkJobExecution milkJobExecution) {
        // is the command Exist ? 
        return new ArrayList<>();
    }

    @Override
    public MilkPlugInDescription getDefinitionDescription(MilkJobContext milkJobContext) {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();
        plugInDescription.setName("ProcessManagerCategory");
        plugInDescription.setLabel("Generate the Process Manager Mapping and Category");
        plugInDescription.setExplanation("Generate automatically the process manager and affect the process in category");
        
        plugInDescription.setCategory(CATEGORY.PROCESSES);
        plugInDescription.setStopJob(JOBSTOPPER.BOTH);

        plugInDescription.addParameter(cstParamProcessManagerEnable);
        plugInDescription.addParameter(cstParamCategoryEnable);
        plugInDescription.addParameter(cstParamCategoryPolicy);

        return plugInDescription;
    }

    @Override
    public int getDefaultNbSavedExecution() {
        return 7;
    }

    @Override
    public int getDefaultNbHistoryMesures() {
        return 14;
    }

    

    @Override
    public MilkJobOutput executeJob(MilkJobExecution milkJobExecution) {
        MilkJobOutput milkJobOutput = milkJobExecution.getMilkJobOutput();
        logger.info(LOGGER_LABEL + " Start MilkProcessManagerCategory");

        //preparation
        ProcessAPI processAPI = milkJobExecution.getApiAccessor().getProcessAPI();

    
        try {
            Boolean processManager = milkJobExecution.getInputBooleanParameter(cstParamProcessManagerEnable);
            Boolean category = milkJobExecution.getInputBooleanParameter(cstParamCategoryEnable);
            
            String categoryPolicy = milkJobExecution.getInputStringParameter(cstParamCategoryPolicy);
            boolean createCategoryIfMissing = CSTCATEGORYPOLICY_CREATEIFMISSING.equals( categoryPolicy);
            int maxProcess=milkJobExecution.getJobStopAfterMaxItems() + 1;
            // default value
            if (maxProcess<1)
                maxProcess=10000;
            BonitaStoreAccessor bonitaStoreAccessor = BonitaStoreAccessor.getInstance(  milkJobExecution.getApiAccessor() );
            SearchOptionsBuilder sob = new SearchOptionsBuilder(0, maxProcess);
            sob.sort( ProcessDeploymentInfoSearchDescriptor.DEPLOYMENT_DATE, Order.DESC);
            List<ProcessDeploymentInfo> listProcessDeployment = processAPI.searchProcessDeploymentInfos(sob.done()).getResult();
            
            milkJobOutput.addReportTableBegin(new String[] { "Process Name", "Version", "Process Manager", "Category" }, 100);

            milkJobExecution.setAvancementStep(listProcessDeployment.size() );
            for (ProcessDeploymentInfo pod  : listProcessDeployment ) {
                if (milkJobExecution.isStopRequired())
                    break;
                milkJobExecution.setAvancementStepPlusOne();
                
                List<BEvent> listEventsProcessManager = null; 
                List<BEvent> listEventsCategory = null; 
                
                if (Boolean.TRUE.equals(processManager)) {
                    listEventsProcessManager = DeployStrategyProcess.applyProcessManager( pod.getProcessId(), bonitaStoreAccessor);
                }
                if (Boolean.TRUE.equals(category)) {
                    ProcessDefinition processDefinition = processAPI.getProcessDefinition(pod.getProcessId());                    
                    listEventsCategory = DeployStrategyProcess.applyCategory(processDefinition, createCategoryIfMissing, bonitaStoreAccessor);
                }
                if (( listEventsProcessManager!=null && ! listEventsProcessManager.isEmpty() ) 
                        || (listEventsCategory!=null && ! listEventsCategory.isEmpty()))
                    milkJobOutput.addItemsProcessed( 1 );
                
                // describe the line
                Object[] lineReport = new Object[] { pod.getName(), pod.getVersion(),
                        listEventsProcessManager==null || listEventsProcessManager.isEmpty() ? "" : BEventFactory.getSyntheticHtml( listEventsProcessManager ),
                                listEventsCategory==null || listEventsCategory.isEmpty()? "" :BEventFactory.getSyntheticHtml( listEventsCategory ),
                                };
                milkJobOutput.addReportTableLine( lineReport );
            }
            milkJobOutput.addReportTableEnd();
        } catch (Exception e) {
            milkJobOutput.addEvent(new BEvent(eventSearchFailed, e, ""));
            milkJobOutput.setExecutionStatus( ExecutionStatus.ERROR );
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe(LOGGER_LABEL + " Error[" + e.getMessage() + "] at " + exceptionDetails);
            return milkJobOutput;
        }

        return milkJobOutput;
    }
}
