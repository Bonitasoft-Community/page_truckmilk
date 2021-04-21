/* ******************************************************************************** */
/*                                                                                  */
/*    TruckMilk PlugIn MilkPurgeArchiveCase                                         */
/*                                                                                  */
/*                                                                                  */
/* ******************************************************************************** */
package org.bonitasoft.truckmilk.plugin.cases;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter.FilterProcess;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.plugin.cases.purge.PurgeDirect;
import org.bonitasoft.truckmilk.plugin.cases.purge.PurgeList;

public class MilkPurgeArchivedCases extends MilkPlugIn {

    static Logger logger = Logger.getLogger(MilkPurgeArchivedCases.class.getName());
    private static final String LOGGER_LABEL = "MilkPurgeArchivedCases ";

    public static final String CST_PLUGIN_NAME = "PurgeArchivedCase";

    private static final String CSTOPERATION_GETLIST = "Get List (No operation)";
    private static final String CSTOPERATION_DIRECT = "Purge";
    private static final String CSTOPERATION_FROMLIST = "Purge from the CSV list";

    public static final String CSTSCOPE_ROOTPROCESS = "Root process information";
    public static final String CSTSCOPE_TRANSIENTONLY = "Transient process information (sub process)";
    public static final String CSTSCOPE_BOTH = "Both";

    public static final String CSTTYPEPURGE_ALL = "all";
    public static final String CSTTYPEPURGE_PARTIALPURGE = "partial";

    
    public static final String CST_YES = "Yes";
    public static final String CST_ALL = "All";
    public static final String CST_KEEPLASTARCHIVE = "Keep last value";
    public static final String CST_NO = "No";

  
  
     private static BEvent eventUnknowOperation = new BEvent(MilkPurgeArchivedCases.class.getName(), 6, Level.APPLICATIONERROR,
            "Operation unknown", "The operation is unknow, only [" + CSTOPERATION_GETLIST + "], [" + CSTOPERATION_DIRECT + "], [" + CSTOPERATION_FROMLIST + "] are known", "No operation is executed", "Check operation");

   

    private static PlugInParameter cstParamOperation = PlugInParameter.createInstanceListValues("operation", "operation: Build a list of cases to operate, do directly the operation, or do the operation from a list",
            new String[] { CSTOPERATION_GETLIST, CSTOPERATION_DIRECT, CSTOPERATION_FROMLIST }, CSTOPERATION_DIRECT, "Result is a purge, or build a list, or used the uploaded list");

    
    public static PlugInParameter cstParamDelay = PlugInParameter.createInstanceDelay("delayinday", "Delay", DELAYSCOPE.MONTH, 3, "The case must be older than this number, in days. 0 means all archived case is immediately in the perimeter")
            .withMandatory(true)
            .withVisibleConditionParameterValueDiff(cstParamOperation, CSTOPERATION_FROMLIST);

    public static PlugInParameter cstParamProcessFilter = PlugInParameter.createInstance("processfilter", "Process Filter", TypeParameter.ARRAYPROCESSNAME, null, "Give a list of process name. Name must be exact, no version is given (all versions will be purged)")
            // .withVisibleCondition("milkJob.parametersvalue[ 'operation' ] != '" + CSTOPERATION_FROMLIST + "'")
            .withVisibleConditionParameterValueDiff(cstParamOperation, CSTOPERATION_FROMLIST).withFilterProcess(FilterProcess.ALL);

    public static PlugInParameter cstParamOperationScopeProcess = PlugInParameter.createInstanceListValues("scope", "Partial SubProcess purge",
            new String[] { CSTSCOPE_ROOTPROCESS, CSTSCOPE_TRANSIENTONLY, CSTSCOPE_BOTH }, CSTSCOPE_ROOTPROCESS,
            CSTSCOPE_ROOTPROCESS + ": Only Root Process are in the scope. In case of Purge, purge a root process purge all sub process information. If you have a FUNDTRANSFERT call as a subprocess, it will not be purged."
                    + CSTSCOPE_TRANSIENTONLY + ": Sub process information is in the scope, but not the root case. For example, you have a process FUNDTRANSFERT, call in a EXPENSE process; Reference the process FUNDTRANSFERT and select this option. All information about FUNDTRANSFERT called as a sub process is in the scope, but not  but not EXPENSE or FUNDTRANSFERT called as a root process;"
                    + CSTSCOPE_BOTH + ": Both root and transient are in the scope. Then a FUNDTRANSFERT information call as root or as a sub process are in the scope")
            .withVisibleCondition("milkJob.parametersvalue[ 'processfilter' ].length > 0");

    public static PlugInParameter cstParamSeparatorCSV = PlugInParameter.createInstance("separatorCSV", "Separator CSV", TypeParameter.STRING, ",", "CSV use a separator. May be ; or ,").withVisibleCondition("milkJob.parametersvalue[ 'operation' ] != '" + CSTOPERATION_DIRECT + "'");

    public static PlugInParameter cstParamListOfCasesDocument = PlugInParameter.createInstanceFile("report", "List of cases", TypeParameter.FILEREADWRITE, null, "List is calculated and saved in this parameter", "ListToPurge.csv", "application/CSV")
            .withVisibleCondition("milkJob.parametersvalue[ 'operation' ] != '" + CSTOPERATION_DIRECT + "'");

    public static final PlugInMeasurement cstMesureCasePurged = PlugInMeasurement.createInstance("CasePurged", "cases purged", "Number of case purged in this execution");
    public static final PlugInMeasurement cstMesureCaseDetected = PlugInMeasurement.createInstance("CaseDetected", "cases detected", "Number of case detected in the scope");

    public static PlugInParameter cstParamTypePurge= PlugInParameter.createInstanceListValues("typePurge", "Type of purge",
            new String[] { CSTTYPEPURGE_ALL, CSTTYPEPURGE_PARTIALPURGE }, CSTTYPEPURGE_ALL, "Delete the case, or partial infromatino in the case")
                        .withVisibleConditionParameterValueDiff(cstParamOperation, CSTOPERATION_GETLIST);

    
    public static PlugInParameter cstParamPurgeContract= PlugInParameter.createInstanceListValues("purgeContract", "Contract",
            new String[] { CST_YES, CST_NO }, CST_NO, "On a user task, save all information that user provided")
                        .withVisibleConditionParameterValueEqual(cstParamTypePurge, CSTTYPEPURGE_PARTIALPURGE);
                        
    public static PlugInParameter cstParamPurgeDataInstance= PlugInParameter.createInstanceListValues("dateInstance", "Data Instance",
            new String[] { CST_ALL, CST_KEEPLASTARCHIVE, CST_NO }, CST_NO, "On a case, a process instance saved data (activity data and process data); A data has multiple archive (one per modification), purge all or keep the last one")
                        .withVisibleConditionParameterValueEqual(cstParamTypePurge, CSTTYPEPURGE_PARTIALPURGE);

    public static PlugInParameter cstParamPurgeDocument= PlugInParameter.createInstanceListValues("purgeDocument", "Document",
            new String[] { CST_YES, CST_NO }, CST_NO, "Process instance manipulate and saved data")
                        .withVisibleConditionParameterValueEqual(cstParamTypePurge, CSTTYPEPURGE_PARTIALPURGE);

    public static PlugInParameter cstParamPurgeActivity= PlugInParameter.createInstanceListValues("purgeActivities", "Activities",
            new String[] { CST_YES, CST_NO }, CST_NO, "All activities are purged. ")
                        .withVisibleConditionParameterValueEqual(cstParamTypePurge, CSTTYPEPURGE_PARTIALPURGE);

    public static PlugInParameter cstParamPurgeBusinessAttachement= PlugInParameter.createInstanceListValues("purgeBusinessAttachement", "Business Attachement",
            new String[] { CST_YES, CST_NO }, CST_NO, "Business Attachement are purged not the business data himself")
                        .withVisibleConditionParameterValueEqual(cstParamTypePurge, CSTTYPEPURGE_PARTIALPURGE);

    public static PlugInParameter cstParamPurgeComment= PlugInParameter.createInstanceListValues("purgeComment", "Comment",
            new String[] { CST_YES, CST_NO }, CST_NO, "Purge comment on the case")
                        .withVisibleConditionParameterValueEqual(cstParamTypePurge, CSTTYPEPURGE_PARTIALPURGE);

    public static PlugInParameter cstParamPurgeSubProcess= PlugInParameter.createInstanceListValues("purgeSubProcess", "Subprocess",
            new String[] { CST_YES, CST_NO }, CST_NO, "Purge subprocess, andd infomation attached to the subprocess (sub data, sud documents, etc...")
                        .withVisibleConditionParameterValueEqual(cstParamTypePurge, CSTTYPEPURGE_PARTIALPURGE);

 
    
    public MilkPurgeArchivedCases() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    @Override
    public ReplacementPlugIn getReplacement(String plugInName, Map<String, Object> parameters) {

        if ("ListPurgeCase".equals(plugInName)) {
            ReplacementPlugIn replacementPlugIn = new ReplacementPlugIn(this, parameters);
            replacementPlugIn.parameters.put(cstParamOperation.name, CSTOPERATION_GETLIST);
            return replacementPlugIn;
        }
        if ("PurgeCaseFromList".equals(plugInName)) {
            ReplacementPlugIn replacementPlugIn = new ReplacementPlugIn(this, parameters);
            replacementPlugIn.parameters.put(cstParamOperation.name, CSTOPERATION_FROMLIST);
            return replacementPlugIn;
        }

        return null;

    }

    /**
     * check the environment : for the milkEmailUsersTasks, we require to be able to send an email
     */
    public List<BEvent> checkPluginEnvironment(MilkJobExecution milkJobExecution) {
        return new ArrayList<>();
    }

    /**
     * check the Job's environment
     */
    public List<BEvent> checkJobEnvironment(MilkJobExecution milkJobExecution) {
        return new ArrayList<>();
    }

    @Override
    public MilkPlugInDescription getDefinitionDescription(MilkJobContext milkJobContext) {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();
        plugInDescription.setName(CST_PLUGIN_NAME);
        plugInDescription.setLabel("Purge Archived Case");
        plugInDescription.setCategory(CATEGORY.CASES);
        plugInDescription.setExplanation("3 operations: PURGE/ GET LIST / PURGE FROM LIST. Purge (or get the list of) archived case according the filter. Filter based on different process, and purge cases older than the delai. At each round with Purge / Purge From list, a maximum case are deleted. If the maximum is over than 100000, it's reduce to this limit.");
        plugInDescription.setWarning("A case purged can't be retrieved. Operation is final. Use with caution.");
        plugInDescription.addParameter( cstParamOperation);
        plugInDescription.addParameter( cstParamDelay);
        plugInDescription.addParameter( cstParamProcessFilter);
        plugInDescription.addParameter( cstParamOperationScopeProcess);

        plugInDescription.addParameter( cstParamSeparatorCSV);
        plugInDescription.addParameter( cstParamListOfCasesDocument);
        
        plugInDescription.addParameter( cstParamTypePurge );
        plugInDescription.addParameter( cstParamPurgeContract );
        plugInDescription.addParameter( cstParamPurgeDataInstance );
        plugInDescription.addParameter( cstParamPurgeDocument );
        plugInDescription.addParameter( cstParamPurgeActivity );
        plugInDescription.addParameter( cstParamPurgeBusinessAttachement );
        plugInDescription.addParameter( cstParamPurgeComment );
        plugInDescription.addParameter( cstParamPurgeSubProcess );
    

        plugInDescription.addMesure(cstMesureCasePurged);
        plugInDescription.addMesure(cstMesureCaseDetected);

        plugInDescription.setStopJob(JOBSTOPPER.BOTH);
        plugInDescription.setJobStopMaxItems(100000);
        return plugInDescription;
    }

    @Override
    public MilkJobOutput executeJob(MilkJobExecution jobExecution) {
        PurgeList purgeList = new PurgeList();
        PurgeDirect purgeDirect = new PurgeDirect();
                
        String operation = jobExecution.getInputStringParameter(cstParamOperation);
        if (CSTOPERATION_DIRECT.equals(operation))
            return purgeDirect.operationDirectPurge(jobExecution);
        else if (CSTOPERATION_GETLIST.equals(operation))
            return purgeList.getList(jobExecution);
        else if (CSTOPERATION_FROMLIST.equals(operation))
            return purgeList.fromList(jobExecution);

        MilkJobOutput milkJobOutput = jobExecution.getMilkJobOutput();
        milkJobOutput.addEvent(new BEvent(eventUnknowOperation, "Operation[" + operation + "]"));
        milkJobOutput.setExecutionStatus(ExecutionStatus.BADCONFIGURATION);
        return milkJobOutput;
    }

   

   
   

  

  

   
   

    
   
}
