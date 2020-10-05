package org.bonitasoft.truckmilk.plugin.monitor;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfo;
import org.bonitasoft.engine.bpm.process.ProcessInstance;
import org.bonitasoft.engine.bpm.process.ProcessInstanceSearchDescriptor;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter.FilterProcess;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.job.MilkJobExecution.ListProcessesResult;

public class MilkDirectory extends MilkPlugIn {


    private static PlugInParameter cstParamDirectory = PlugInParameter.createInstance("directory", "Directory", TypeParameter.STRING, "", "This directory is monitor. Each file detected in this directory which match the filter trigger an action (create a case)")
            .withMandatory(true);

    private static PlugInParameter cstParamProcessFilter = PlugInParameter.createInstance("processfilter", "Filter on process", TypeParameter.ARRAYPROCESSNAME, null, "Job manage only process which mach the filter. If no filter is given, all processes are inspected")
            .withFilterProcess(FilterProcess.ONLYENABLED);

    private static PlugInParameter cstParamFileFilter = PlugInParameter.createInstance("fileFilter", "File Filter", TypeParameter.STRING, "[0-z|\\s]*.doc", "File filter, to load only file who match the filter. * and ? may be used.Visit https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html#sum");

    private static BEvent eventDirectoryNotExist = new BEvent(MilkDirectory.class.getName(), 1, Level.APPLICATIONERROR,
            "Directory not exist", "The directory given as parameters does not exist", "Jobs can't run", "Check parameters");

    /**
     * 
     */
    public MilkDirectory() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * check the environment : for the deleteCase, nothing is required
     */
    public List<BEvent> checkPluginEnvironment(MilkJobExecution milkJobExecution) {
        return new ArrayList<>();
    };

    /**
     * check the environment : for the milkDirectory,
     * The path to check depends on the tour, so the only environment needed is to access a disk ?
     */
    public List<BEvent> checkJobEnvironment(MilkJobExecution milkJobExecution) {
        List<BEvent> listEvents = new ArrayList<>();
        File fileDirectoy = new File(milkJobExecution.getInputStringParameter(cstParamDirectory));
        if (!fileDirectoy.isDirectory())
            listEvents.add(new BEvent(eventDirectoryNotExist, milkJobExecution.getInputStringParameter(cstParamDirectory)));
        return listEvents;
    };

    @Override
    public MilkPlugInDescription getDefinitionDescription(MilkJobContext milkJobContext) {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();
        plugInDescription.setName("MonitorDirectory");
        plugInDescription.setLabel("Monitor Directory");
        plugInDescription.setExplanation("Monitor a directory. When a file arrive in this directory, a new case is created");

        plugInDescription.setCategory(CATEGORY.MONITOR);
        plugInDescription.addParameter(cstParamDirectory);
        plugInDescription.addParameter(cstParamFileFilter);
        plugInDescription.addParameter(cstParamProcessFilter);
        plugInDescription.setStopJob(JOBSTOPPER.BOTH);

        return plugInDescription;
    }

    @Override
    public MilkJobOutput executeJob(MilkJobExecution milkJobExecution) {
        MilkJobOutput milkJobOutput = milkJobExecution.getMilkJobOutput();
        int numberOfCasesCreated=0;
        int numberOfCasesInError=0;

        try {
            ProcessAPI processAPI = milkJobExecution.getApiAccessor().getProcessAPI();

            // ---- get parameters
            ListProcessesResult listProcessResult = milkJobExecution.getInputArrayProcess(cstParamProcessFilter, false, null, ProcessInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processAPI);

            if (BEventFactory.isError(listProcessResult.listEvents)) {
                // filter given, no process found : stop now
                milkJobOutput.addEvents(listProcessResult.listEvents);
                milkJobOutput.executionStatus = ExecutionStatus.BADCONFIGURATION;
                return milkJobOutput;
            }
            String fileFilter = milkJobExecution.getInputStringParameter(cstParamFileFilter);
            Pattern patternFileFilter = null;
            if (fileFilter != null && ! fileFilter.trim().isEmpty())
                patternFileFilter = Pattern.compile(fileFilter);
            
            File fileDirectoy = new File(milkJobExecution.getInputStringParameter(cstParamDirectory));
            milkJobOutput.addReportTableBegin(new String[] { "File", "CaseId" });
            
            // ------------ loop now
            File[] listFiles = fileDirectoy.listFiles();
            milkJobExecution.setAvancementTotalStep(listFiles.length);

            for (File fileInDirectory : listFiles) {
                milkJobExecution.setAvancementStep(1);
                
                Thread.sleep( 10000 );
                
                if (milkJobExecution.isStopRequired())
                    break;
                if (fileInDirectory.isDirectory())
                    continue;
                // is the file match the filter?
                if (patternFileFilter !=null) {
                    Matcher m = patternFileFilter.matcher(fileInDirectory.getName());
                    if (!m.matches())
                        continue;
                }

                
                // Ok, get it ! Create something 
                Map<String, Object> parameters = new HashMap<>();
                parameters.put("FILENAME", fileInDirectory.getName());
                parameters.put("FILEDATEINMS", fileInDirectory.lastModified());
                List<File> fileAttachement = new ArrayList<>();
                fileAttachement.add(fileInDirectory);

                for (ProcessDeploymentInfo processDeployment : listProcessResult.listProcessDeploymentInfo) {
                    try {
                        ProcessInstance processInstance = processAPI.startProcess(processDeployment.getProcessId());
                        milkJobOutput.addReportTableLine(new Object[] { fileInDirectory.getName(), processInstance.getId() });
                        numberOfCasesCreated++;
                    } catch (Exception e) {
                        milkJobOutput.addReportTableLine(new Object[] { fileInDirectory.getName(), e.getMessage() });
                        numberOfCasesInError++;
                    }
                }

                // Delete the file
                fileInDirectory.delete();

            }
            milkJobOutput.addReportTableEnd();

        } catch (Exception e) {
            milkJobOutput.addReportInHtml("Exception "+e.getMessage());
        }
        
        // calculate the result now
        if (numberOfCasesInError>0)
            milkJobOutput.executionStatus = ExecutionStatus.ERROR;
        else if (milkJobExecution.isStopRequired())
            milkJobOutput.executionStatus = ExecutionStatus.SUCCESSPARTIAL;
        else if (numberOfCasesCreated + numberOfCasesInError==0)
            milkJobOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
        else
            milkJobOutput.executionStatus = ExecutionStatus.SUCCESS;
        return milkJobOutput;
    }

}
