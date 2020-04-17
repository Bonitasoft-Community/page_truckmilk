package org.bonitasoft.truckmilk.plugin;

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
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.mapper.Mapper;
import org.bonitasoft.truckmilk.mapper.Mapper.MAPPER_POLICY;
import org.bonitasoft.truckmilk.toolbox.CSVOperation;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;

public class MilkDirectory extends MilkPlugIn {

    private static String OPERATIONPOLICY_FILE = "Operation on File";
    private static String OPERATIONPOLICY_CSVONECASE = "Operation on CSV, One case per file";
    private static String OPERATIONPOLICY_CSVONECASEPERLINE = "Operation on CSV, One case per line in the CSV";
    
    private static String[] OPERATIONPOLICY = { OPERATIONPOLICY_FILE, OPERATIONPOLICY_CSVONECASE,OPERATIONPOLICY_CSVONECASEPERLINE };

    private static String ARCHIVEPOLICY_MOVETOARCHIVEDDIRECTORY = "Archive";
    private static String ARCHIVEPOLICY_DELETE = "Delete";
    private static String ARCHIVEPOLICY_MOVETOARCHIVEDDIRECTORYANDPURGE = "Archive, then purge ";
    
    private static String[] ARCHIVEPOLICY = { ARCHIVEPOLICY_MOVETOARCHIVEDDIRECTORY, ARCHIVEPOLICY_DELETE,ARCHIVEPOLICY_MOVETOARCHIVEDDIRECTORYANDPURGE };

    private static PlugInParameter cstParamDirectory = PlugInParameter.createInstance("directory", "Directory", TypeParameter.STRING, "", "This directory is monitor. Each file detected in this directory which match the filter trigger an action (create a case)", true);
    
    private static PlugInParameter cstParamOperation = PlugInParameter.createInstanceListValues("operation", "Operation", OPERATIONPOLICY, OPERATIONPOLICY_FILE, "when a file is detected, operation running. "
    + OPERATIONPOLICY_FILE +": one case is created per file. "
    + OPERATIONPOLICY_CSVONECASE +": one case is created per file, attributes can be read in the first CSV line."
    + OPERATIONPOLICY_CSVONECASEPERLINE+": one case is created per line in the CSV file; if the CSV is empty, to case are created"
    );
    
    private static PlugInParameter cstParamFileFilter = PlugInParameter.createInstance("fileFilter", "File Filter", TypeParameter.STRING, "*.*", "File filter, to load only file who match the filter. * and ? may be used");
    private static PlugInParameter cstParamArchiveFilePolicy = PlugInParameter.createInstanceListValues("archivePolicy", "Archive policy", ARCHIVEPOLICY, ARCHIVEPOLICY_MOVETOARCHIVEDDIRECTORY, 
            "When the file is processed, policy to apply: "
            + ARCHIVEPOLICY_DELETE+": file(s) are immediately deleted."
            + ARCHIVEPOLICY_MOVETOARCHIVEDDIRECTORY+": file(s) are moved to the archive directory, specified in the parameters"
            + ARCHIVEPOLICY_MOVETOARCHIVEDDIRECTORYANDPURGE+": file(s) are moved to the archive directory, then purge after a delay."
            );
    private static PlugInParameter cstParamDirectoryArchive = PlugInParameter.createInstance("directoryarchive", "Directory Archive", TypeParameter.STRING, "", "When a file is used, it will be moved in this directory. If no directory is set, file is removed");
    private static PlugInParameter cstParamDelayArchive = PlugInParameter.createInstance("delayarchive", "Delay before purging Archive Directory", TypeParameter.DELAY,MilkPlugInToolbox.DELAYSCOPE.MONTH + ":1", "Files older than this delay in the archive directory are definitively purged");

    private static PlugInParameter cstParamCsvSeparator = PlugInParameter.createInstance("csvSeparator", "Csv Separator", TypeParameter.STRING, ",", "Separator expected to separate the different value (in the header, in data)");

    Mapper mapper = new Mapper(MAPPER_POLICY.CASECREATION);

    private static BEvent EVENT_DIRECTORY_NOT_EXIST = new BEvent(MilkDirectory.class.getName(), 1, Level.APPLICATIONERROR,
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
    public List<BEvent> checkPluginEnvironment(MilkJobExecution jobExecution) {
        return new ArrayList<>();
    };

    /**
     * check the environment : for the milkDirectory,
     * The path to check depends on the tour, so the only environment needed is to access a disk ?
     */
    public List<BEvent> checkJobEnvironment(MilkJobExecution jobExecution) {
        List<BEvent> listEvents = new ArrayList<>();
        File fileDirectoy = new File(jobExecution.getInputStringParameter(cstParamDirectory));
        if (!fileDirectoy.isDirectory())
            listEvents.add(new BEvent(EVENT_DIRECTORY_NOT_EXIST, jobExecution.getInputStringParameter(cstParamDirectory)));
        return listEvents;
    };
    @Override
    public MilkPlugInDescription getDefinitionDescription() {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();
        plugInDescription.setName( "MonitorDirectory");
        plugInDescription.setLabel( "Monitor Directory");
        plugInDescription.setExplanation( "Monitor a directory. When a file arrive in this directory, a new case is created");
        
        plugInDescription.setCategory( CATEGORY.MONITOR);
        plugInDescription.addParameter( cstParamDirectory );
        plugInDescription.addParameter( cstParamOperation );
        plugInDescription.addParameter( cstParamFileFilter );
        plugInDescription.addParameter(  cstParamArchiveFilePolicy );
        plugInDescription.addParameter( cstParamDirectoryArchive );
        plugInDescription.addParameter( cstParamDelayArchive );
        plugInDescription.addParameter( cstParamCsvSeparator );

        mapper.addPlugInParameter(plugInDescription);
        
        return plugInDescription;
    }
    @Override
    public MilkJobOutput execute(MilkJobExecution jobExecution) {
        MilkJobOutput plugTourOutput = jobExecution.getMilkJobOutput();
        try {
            String operationPolicy = jobExecution.getInputStringParameter(cstParamOperation);
            String separatorCSV = jobExecution.getInputStringParameter(cstParamCsvSeparator);
            
            
            List<BEvent> listEvents = mapper.initialisation(jobExecution, jobExecution.getApiAccessor());
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

                // Ok, get it ! Create something 
                Map<String, Object> parameters = new HashMap<>();
                parameters.put("FILENAME", fileInDirectory.getName());
                parameters.put("FILEDATEINMS", fileInDirectory.lastModified());
                List<File> fileAttachement = new ArrayList<>();
                fileAttachement.add(fileInDirectory);
                
                
                if (OPERATIONPOLICY_CSVONECASEPERLINE.equals(operationPolicy) && fileInDirectory.getName().toLowerCase().endsWith(".csv")) {
                    CSVOperation csvOperation = new CSVOperation();
                    csvOperation.loadCsvDocument(jobExecution, fileInDirectory, separatorCSV);
                    Map<String, String> record;
                    while (( record =  csvOperation.getNextRecord()) !=null)
                    {
                        Map<String, Object> recordComplete = new HashMap<>();
                        recordComplete.putAll( record );
                        recordComplete.putAll( parameters );
                        mapper.createCase(jobExecution, recordComplete, fileAttachement, jobExecution.getApiAccessor());
                    }
                    // open the CSV
                } else if (OPERATIONPOLICY_CSVONECASE.equals(operationPolicy)) {
                    // open the CSV, complete parameters, but create a case anytime
                    CSVOperation csvOperation = new CSVOperation();
                    csvOperation.loadCsvDocument(jobExecution, fileInDirectory, separatorCSV);
                    Map<String, String> record =  csvOperation.getNextRecord();
                    Map<String, Object> recordComplete = new HashMap<>();
                    if (record!=null)
                        recordComplete.putAll( record );
                    recordComplete.putAll( parameters );
                    mapper.createCase(jobExecution, recordComplete, fileAttachement, jobExecution.getApiAccessor());
                } else if (OPERATIONPOLICY_FILE.equals(operationPolicy)) {
                    // one case per file
                    mapper.createCase(jobExecution, parameters, fileAttachement, jobExecution.getApiAccessor());
                }
                
                // move file to archive directory
            }
            
            // now, purge the archive directory?
            
        } catch (Exception e) {

        }
        return plugTourOutput;
    }

   
}
