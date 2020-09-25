package org.bonitasoft.truckmilk.plugin.monitor;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.mapper.Mapper;
import org.bonitasoft.truckmilk.mapper.Mapper.MAPPER_POLICY;
import org.bonitasoft.truckmilk.toolbox.CSVOperation;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobContext;

public class MilkDirectory extends MilkPlugIn {

    private static String operationPolicyFile = "Operation on File";
    private static String operationPolicyCsvOneCase = "Operation on CSV, One case per file";
    private static String operationPolicyCsvOneOperationPerLine = "Operation on CSV, One case per line in the CSV";



    private static String achivePolicyMoveToArchiveDirectory = "Archive";
    private static String archivePolicyDelete = "Delete";
    private static String archivePolicyMoveToDirectoryThenPurge = "Archive, then purge";

    private static PlugInParameter cstParamDirectory = PlugInParameter.createInstance("directory", "Directory", TypeParameter.STRING, "", "This directory is monitor. Each file detected in this directory which match the filter trigger an action (create a case)")
            .withMandatory(true);

    private static PlugInParameter cstParamOperation = PlugInParameter.createInstanceListValues("operation", "Operation", 
            new String[] { operationPolicyFile, operationPolicyCsvOneCase, operationPolicyCsvOneOperationPerLine },
            operationPolicyFile, "when a file is detected, operation running. "
            + operationPolicyFile + ": one operation (create case, execute task, ...) is performed per file. "
            + operationPolicyCsvOneCase + ": one operation is performed per file, attributes can be read in the first CSV line."
            + operationPolicyCsvOneOperationPerLine + ": one operation is performed per line in the CSV file; if the CSV is empty, to case are created");

    private static PlugInParameter cstParamFileFilter = PlugInParameter.createInstance("fileFilter", "File Filter", TypeParameter.STRING, "*.*", "File filter, to load only file who match the filter. * and ? may be used");
    private static PlugInParameter cstParamArchiveFilePolicy = PlugInParameter.createInstanceListValues("archivePolicy", "Archive policy",
            new String[] { achivePolicyMoveToArchiveDirectory, archivePolicyDelete, archivePolicyMoveToDirectoryThenPurge }, 
            achivePolicyMoveToArchiveDirectory,
            "When the file is processed, policy to apply: "
                    + archivePolicyDelete + ": file(s) are immediately deleted."
                    + achivePolicyMoveToArchiveDirectory + ": file(s) are moved to the archive directory, specified in the parameters"
                    + archivePolicyMoveToDirectoryThenPurge + ": file(s) are moved to the archive directory, then purge after a delay.");
    private static PlugInParameter cstParamDirectoryArchive = PlugInParameter.createInstance("directoryarchive", "Directory Archive", TypeParameter.STRING, "", "When a file is used, it will be moved in this directory. If no directory is set, file is removed")
            .withVisibleCondition(" milkJob.parametersvalue[ 'archivePolicy' ] === 'Archive' || milkJob.parametersvalue[ 'archivePolicy' ] === 'Archive, then purge'");
    private static PlugInParameter cstParamDelayArchive = PlugInParameter.createInstanceDelay("delayarchive", "Delay before purging Archive Directory", DELAYSCOPE.MONTH, 1, "Files older than this delay in the archive directory are definitively purged")
            .withVisibleCondition("milkJob.parametersvalue[ 'archivePolicy' ] === 'Archive, then purge'");
    private static PlugInParameter cstParamCsvSeparator = PlugInParameter.createInstance("csvSeparator", "Csv Separator", TypeParameter.STRING, ",", "Separator expected to separate the different value (in the header, in data)");

    
    
    Mapper mapper = new Mapper(MAPPER_POLICY.BOTH);

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
        plugInDescription.addParameter(cstParamOperation);
        plugInDescription.addParameter(cstParamFileFilter);
        plugInDescription.addParameter(cstParamArchiveFilePolicy);
        plugInDescription.addParameter(cstParamDirectoryArchive);
        plugInDescription.addParameter(cstParamDelayArchive);
        plugInDescription.addParameter(cstParamCsvSeparator);
        plugInDescription.setStopJob( JOBSTOPPER.BOTH);
        mapper.addPlugInParameter(plugInDescription);

        return plugInDescription;
    }

    @Override
    public MilkJobOutput executeJob(MilkJobExecution jobExecution) {
        MilkJobOutput plugTourOutput = jobExecution.getMilkJobOutput();
        try {
            StringBuilder analysis = new StringBuilder();
            String operationPolicy = jobExecution.getInputStringParameter(cstParamOperation);
            String separatorCSV = jobExecution.getInputStringParameter(cstParamCsvSeparator);

            List<BEvent> listEvents = mapper.initialisation(jobExecution);
            if (BEventFactory.isError(listEvents)) {
                plugTourOutput.addEvents(listEvents);
                plugTourOutput.executionStatus = ExecutionStatus.ERROR;
                return plugTourOutput;

            }
            String fileFilter = jobExecution.getInputStringParameter(cstParamFileFilter);

            File fileDirectoy = new File(jobExecution.getInputStringParameter(cstParamDirectory));
            File[] listFiles = fileDirectoy.listFiles();
            jobExecution.setAvancementTotalStep(listFiles.length);
            for (File fileInDirectory : listFiles) {
                jobExecution.setAvancementStep(1);
                if (fileInDirectory.isDirectory())
                    continue;
                // is the file match the filter?
                if (fileFilter!=null && fileFilter.length()>0 && ! fileInDirectory.getName().matches(fileFilter))
                {
                    analysis.append("File["+fileInDirectory.getName()+"] does not match;");
                    continue;
                }
                // Ok, get it ! Create something 
                Map<String, Object> parameters = new HashMap<>();
                parameters.put("FILENAME", fileInDirectory.getName());
                parameters.put("FILEDATEINMS", fileInDirectory.lastModified());
                List<File> fileAttachement = new ArrayList<>();
                fileAttachement.add(fileInDirectory);

                if (operationPolicyCsvOneOperationPerLine.equals(operationPolicy)) {
                    if (! fileInDirectory.getName().toLowerCase().endsWith(".csv")) {
                        analysis.append("File["+fileInDirectory.getName()+"] is not a CSV file;");
                        continue;                    
                    }
                    CSVOperation csvOperation = new CSVOperation();
                    csvOperation.loadCsvDocument(jobExecution, fileInDirectory, separatorCSV);
                    Map<String, String> record;
                    while ((record = csvOperation.getNextRecord()) != null) {
                        Map<String, Object> recordComplete = new HashMap<>();
                        recordComplete.putAll(record);
                        recordComplete.putAll(parameters);
                        mapper.performOperation(jobExecution, recordComplete, fileAttachement);
                    }
                    // open the CSV
                } else if (operationPolicyCsvOneCase.equals(operationPolicy)) {
                    if (! fileInDirectory.getName().toLowerCase().endsWith(".csv")) {
                        analysis.append("File["+fileInDirectory.getName()+"] is not a CSV file;");
                        continue;                    
                    }

                    // open the CSV, complete parameters, but create a case anytime
                    CSVOperation csvOperation = new CSVOperation();
                    csvOperation.loadCsvDocument(jobExecution, fileInDirectory, separatorCSV);
                    Map<String, String> record = csvOperation.getNextRecord();
                    Map<String, Object> recordComplete = new HashMap<>();
                    if (record != null)
                        recordComplete.putAll(record);
                    recordComplete.putAll(parameters);
                    mapper.performOperation(jobExecution, recordComplete, fileAttachement );
                } else if (operationPolicyFile.equals(operationPolicy)) {
                    // one case per file
                    mapper.performOperation(jobExecution, parameters, fileAttachement);
                }

                // move file to archive directory
            }

            // now, purge the archive directory?

        } catch (Exception e) {

        }
        return plugTourOutput;
    }

}
