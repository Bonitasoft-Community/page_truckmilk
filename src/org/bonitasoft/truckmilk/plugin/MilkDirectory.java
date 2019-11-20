package org.bonitasoft.truckmilk.plugin;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.ExecutionStatus;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.mapper.Mapper;
import org.bonitasoft.truckmilk.mapper.Mapper.MAPPER_POLICY;
import org.quartz.JobExecutionContext;

public class MilkDirectory extends MilkPlugIn {

    private static String CSVPOLICY_ONECASEPERLINE= "ONECASEPERLINE";
    private static String CSVPOLICY_ONECASEPERFILE= "ONECASEPERFILE";
    private static String[] CSVPOLICY = {CSVPOLICY_ONECASEPERFILE, CSVPOLICY_ONECASEPERLINE};
    
    private static PlugInParameter cstParamDirectory = PlugInParameter.createInstance("directory", "Directory", TypeParameter.STRING, "", "This directory is monitor. Each file detected in this directory which match the filter trigger an action (create a case)");
    private static PlugInParameter cstParamDirectoryArchive = PlugInParameter.createInstance("directoryarchive", "Directory Archive", TypeParameter.STRING, "", "When a file is used, it will be moved in this directory. If no directory is set, file is removed");
    private static PlugInParameter cstParamFileFiler = PlugInParameter.createInstance("fileFilter", "File Filter", TypeParameter.STRING, "*.*", "File filter, to load only file who match the filter. * and ? may be used");
    private static PlugInParameter cstParamCsvFilePolicy = PlugInParameter.createInstanceListValues("csvFilePolicy", "Csv file Policy", CSVPOLICY, CSVPOLICY_ONECASEPERLINE, "ONECASE : one case is created by the file. ONECASEPERLINE : CSV is openned, one case is created per line in the CSV (a Header is expected)");
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
    public List<BEvent> checkPluginEnvironment(long tenantId, APIAccessor apiAccessor) {
        return new ArrayList<BEvent>();
    };
    
    /**
     * check the environment : for the milkDirectory, 
     * The path to check depends on the tour, so the only environment needed is to access a disk ?
     */
    public List<BEvent> checkJobEnvironment(MilkJobExecution jobExecution, APIAccessor apiAccessor) {
        List<BEvent> listEvents = new ArrayList<BEvent>();
        File fileDirectoy = new File( jobExecution.getInputStringParameter(cstParamDirectory));
        if (! fileDirectoy.isDirectory())
            listEvents.add( new BEvent(EVENT_DIRECTORY_NOT_EXIST, jobExecution.getInputStringParameter(cstParamDirectory)));
        return listEvents;
    };

    @Override
    public PlugTourOutput execute(MilkJobExecution jobExecution, APIAccessor apiAccessor) {
        PlugTourOutput plugTourOutput = jobExecution.getPlugTourOutput();
        try
        {
            String csvPolicy    = jobExecution.getInputStringParameter( cstParamCsvFilePolicy);
            String csvSeparator = jobExecution.getInputStringParameter( cstParamCsvSeparator);
            List<BEvent> listEvents = mapper.initialisation(jobExecution, apiAccessor);
            if (BEventFactory.isError( listEvents ))
            {
                plugTourOutput.addEvents( listEvents);
                plugTourOutput.executionStatus = ExecutionStatus.ERROR;
                return plugTourOutput;

            }
            File fileDirectoy = new File( jobExecution.getInputStringParameter(cstParamDirectory));
            File[] listFiles = fileDirectoy.listFiles();
            jobExecution.setAvancementTotalStep( listFiles.length);
            for (File fileInDirectory : listFiles)
            {
                jobExecution.setAvancementStep(1);
                if (fileInDirectory.isDirectory())
                    continue;
                // is the file match the filter?
                
                // Ok, get it ! Create something 
                Map<String,Object> parameters = new HashMap<String,Object>();
                parameters.put("FILENAME", fileInDirectory.getName());
                parameters.put("FILEDATEINMS", fileInDirectory.lastModified());
                List<File> fileAttachement = new ArrayList<File>();
                fileAttachement.add( fileInDirectory );
                if (CSVPOLICY_ONECASEPERLINE.equals( csvPolicy) && fileInDirectory.getName().toLowerCase().endsWith(".csv")) {
                    // open the CSV
                }
                else
                {
                    // one case per file
                    mapper.createCase( jobExecution, parameters, fileAttachement, apiAccessor );
                }
            }
        }
        catch(Exception e)
        {
            
        }
        return plugTourOutput;
    }

    @Override
    public PlugInDescription getDefinitionDescription() {
        PlugInDescription plugInDescription = new PlugInDescription();
        plugInDescription.name = "MonitorDirectory";
        plugInDescription.label = "Monitor Directory";
        plugInDescription.description = "Monitor a directory. When a file arrive in this directory, a new case is created";
        plugInDescription.category = CATEGORY.MONITOR;
        
        return plugInDescription;
    }
}
