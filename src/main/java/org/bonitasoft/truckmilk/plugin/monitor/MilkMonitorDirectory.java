package org.bonitasoft.truckmilk.plugin.monitor;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.mapper.Mapper;
import org.bonitasoft.truckmilk.mapper.Mapper.MAPPER_POLICY;
import org.bonitasoft.truckmilk.mapper.Mapper.MapperResult;

public class MilkMonitorDirectory extends MilkPlugIn {

    private final static String CSTFILEPROCESSED_DELETED="Delete the file after execution";
    private final static String CSTFILEPROCESSED_MOVEARCHIVED="Move the file to a Archive after execution";
    
    private static PlugInParameter cstParamDirectory = PlugInParameter.createInstance("directory", "Directory", TypeParameter.STRING, "", "This directory is monitor. Each file detected in this directory which match the filter trigger an action (create a case)")
            .withMandatory(true);

    private static PlugInParameter cstParamFileFilter = PlugInParameter.createInstance("fileFilter", "File Filter", TypeParameter.STRING, "[0-z|\\s]*.doc", "File filter, to load only file who match the filter. * and ? may be used.Visit https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html#sum");

    private static PlugInParameter cstParamFileProcessedPolicy = PlugInParameter.createInstanceListValues("fimeprocessed", "File processed", 
            new String[] { CSTFILEPROCESSED_DELETED, CSTFILEPROCESSED_MOVEARCHIVED },
            CSTFILEPROCESSED_MOVEARCHIVED,"When a file is processed, it can be removed or moved to an ARCHIVE subdirectory");
    

    private static BEvent eventDirectoryNotExist = new BEvent(MilkMonitorDirectory.class.getName(), 1, Level.APPLICATIONERROR,
            "Directory not exist", "The directory given as parameters does not exist", "Jobs can't run", "Check parameters");
    private static BEvent eventCantCreateArchiveDirectory = new BEvent(MilkMonitorDirectory.class.getName(), 2, Level.APPLICATIONERROR,
            "Can't create archives directory", "A archive directory is required to move file after processing. This directoryu can't be created", "Files can't be moved, job stop", "Check error and directory");

    
    private static BEvent eventCantMoveFile = new BEvent(MilkMonitorDirectory.class.getName(), 3, Level.APPLICATIONERROR,
            "Can't move file to the archive directory", "Move file to the archive directory failed", "This file will be processed again at the next execution", "Check error and directory");
    private static BEvent eventCantDeleteFile = new BEvent(MilkMonitorDirectory.class.getName(), 4, Level.APPLICATIONERROR,
            "Can't delete the file", "Delete file after processing failed", "This file will be processed again at the next execution", "Check error and directory");
    
    /**
     * 
     */
    public MilkMonitorDirectory() {
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
        plugInDescription.addParameter(cstParamFileProcessedPolicy);
        
        plugInDescription.setStopJob(JOBSTOPPER.BOTH);

        Mapper mapper = new Mapper(MAPPER_POLICY.BOTH);
        mapper.addPlugInParameter(plugInDescription, 
                "Additionnal placeholder: {{FILESOURCE}}: content of the source, {{FILENAME}} FileName (not the directory), {{FILEDATEINMS}} date in millisecond.<br> Example, \"{ \"invoiceInput\": \"{{FILESOURCE}}\" }"); 
        
        return plugInDescription;
    }

    @Override
    public MilkJobOutput executeJob(MilkJobExecution milkJobExecution) {
        MilkJobOutput milkJobOutput = milkJobExecution.getMilkJobOutput();
        int numberOfCasesCreated=0;
        int numberOfCasesInError=0;
        
        try {
            Mapper mapper = new Mapper(MAPPER_POLICY.BOTH);
            milkJobOutput.addEvents( mapper.initialisation(milkJobExecution) );
            if (BEventFactory.isError( milkJobOutput.getListEvents())) {
                milkJobOutput.setExecutionStatus( ExecutionStatus.ERROR );
                return milkJobOutput;
            }

            // ---- get parameters
            String fileFilter = milkJobExecution.getInputStringParameter(cstParamFileFilter);
            Pattern patternFileFilter = null;
            if (fileFilter != null && ! fileFilter.trim().isEmpty())
                patternFileFilter = Pattern.compile(fileFilter);
            
            File fileDirectory = new File(milkJobExecution.getInputStringParameter(cstParamDirectory));
            
            
            String fileProcessPolicy = milkJobExecution.getInputStringParameter(cstParamFileProcessedPolicy);
            File fileArchived =null;
            if (CSTFILEPROCESSED_MOVEARCHIVED.equals( fileProcessPolicy)) {
                fileArchived= new File( fileDirectory.getAbsolutePath()+"/archives");
                if (!fileArchived.exists())
                    if (! fileArchived.mkdirs()) {
                        milkJobOutput.addEvent( new BEvent(eventCantCreateArchiveDirectory, "Directory ["+fileArchived.getAbsolutePath()+"]"));
                        return milkJobOutput ;
                    }
            }
            milkJobOutput.addReportTableBegin(new String[] { "File", "CaseId", "Status" });
            
            // ------------ loop now
            File[] listFiles = fileDirectory.listFiles();
            milkJobExecution.setAvancementTotalStep(listFiles.length);

            for (File fileInDirectory : listFiles) {
                milkJobExecution.setAvancementStep(1);
                
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
                parameters.put("FILESOURCE_FILE", fileInDirectory);
                // attention use / else JSONParse will translate the \ in something else
                String completeFileName = fileInDirectory.getAbsolutePath();
                completeFileName = completeFileName.replace('\\', '/');
                parameters.put("FILESOURCE", "file("+completeFileName+")");                
                parameters.put("FILENAME", fileInDirectory.getName());
                
                String fileNameWithoutSuffix = fileInDirectory.getName();
                if (fileNameWithoutSuffix.lastIndexOf(".") != -1)
                    fileNameWithoutSuffix = fileNameWithoutSuffix.substring(0, fileNameWithoutSuffix.lastIndexOf("."));
                parameters.put("FILENAMEWITHOUTSUFFIX",fileNameWithoutSuffix);
                
                parameters.put("FILEDATEINMS", fileInDirectory.lastModified());
                List<File> fileAttachement = new ArrayList<>();
                fileAttachement.add(fileInDirectory);
                boolean deleteFile=false;
                    try {
                        MapperResult mapperResult = mapper.performOperation(milkJobExecution, parameters, fileAttachement);
                        List<BEvent> listEvents = new ArrayList<>();
                        listEvents.addAll(mapperResult.listEvents);
                        if (BEventFactory.isError( listEvents))
                            numberOfCasesInError++;
                        else {
                            numberOfCasesCreated++;
                            deleteFile=true;
                            milkJobOutput.nbItemsProcessed++;
                        }
                        
                        if (deleteFile) {
                            if (CSTFILEPROCESSED_DELETED.equals( fileProcessPolicy)) {
                                if (! fileInDirectory.delete())
                                    listEvents.add( new BEvent(eventCantDeleteFile, "FileName ["+fileInDirectory.getAbsolutePath()+"]"));
                                    
                            } else if (CSTFILEPROCESSED_MOVEARCHIVED.equals( fileProcessPolicy)) {
                                Path source = Paths.get(fileInDirectory.getAbsolutePath());
                                Path target = Paths.get(fileArchived.getAbsolutePath()+ "/"+fileInDirectory.getName());

                                try {

                                    // rename or move a file to other path
                                    // if target exists, throws FileAlreadyExistsException
                                    Files.move(source, target);
                                }
                                catch( Exception e) {
                                    listEvents.add( new BEvent(eventCantMoveFile, e, "FileName ["+fileInDirectory.getAbsolutePath()+"] to ["+target.getFileName().toString()+"] message:"+e.getMessage()));

                                }

                            } else {
                                // Unknown policy, delete 
                                if (! fileInDirectory.delete())
                                    listEvents.add( new BEvent(eventCantDeleteFile, "FileName ["+fileInDirectory.getAbsolutePath()+"]"));
                            }
                        }
                        
                        milkJobOutput.addReportTableLine(new Object[] { fileInDirectory.getName(),
                                (mapperResult.processInstance!=null ? mapperResult.processInstance.getId():""),
                                mapperResult.typeOperation + 
                                (mapperResult.listEvents.isEmpty()? "" : "<br>"+BEventFactory.getHtml( mapperResult.listEvents)) });
                        

                    } catch (Exception e) {
                        milkJobOutput.addReportTableLine(new Object[] { fileInDirectory.getName(), e.getMessage() });
                        numberOfCasesInError++;
                    }

                

            }
            milkJobOutput.addReportTableEnd();

        } catch (Exception e) {
            milkJobOutput.addReportInHtml("Exception "+e.getMessage());
        }
        
        // calculate the result now
        if (numberOfCasesInError>0)
            milkJobOutput.setExecutionStatus( ExecutionStatus.ERROR );
        else if (milkJobExecution.isStopRequired())
            milkJobOutput.setExecutionStatus( ExecutionStatus.SUCCESSPARTIAL );
        else if (numberOfCasesCreated + numberOfCasesInError==0)
            milkJobOutput.setExecutionStatus( ExecutionStatus.SUCCESSNOTHING );
        else
            milkJobOutput.setExecutionStatus( ExecutionStatus.SUCCESS );
        return milkJobOutput;
    }

}
