package org.bonitasoft.truckmilk.plugin.processes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import org.bonitasoft.engine.bpm.process.ActivationState;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstancesSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ProcessActivationException;
import org.bonitasoft.engine.bpm.process.ProcessDefinitionNotFoundException;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfo;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfoSearchDescriptor;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.engine.service.TenantServiceAccessor;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.sonar.engine.SonarConfiguration;
import org.bonitasoft.sonar.engine.SonarCriterias;
import org.bonitasoft.sonar.engine.SonarEngine;
import org.bonitasoft.sonar.engine.SonarItem.SEVERITY;
import org.bonitasoft.sonar.engine.SonarProcess;
import org.bonitasoft.sonar.engine.SonarRule;
import org.bonitasoft.sonar.engine.SonarTrackExecution;
import org.bonitasoft.sonar.engine.result.SonarResult;
import org.bonitasoft.sonar.engine.result.SonarResult.SonarResultProcess;
import org.bonitasoft.sonar.engine.result.SonarResultDecoCsv;
import org.bonitasoft.sonar.engine.result.SonarResultDecoHtml;
import org.bonitasoft.sonar.engine.result.SonarResultDecoStatistics;
import org.bonitasoft.sonar.engine.result.SonarResultDecoStatistics.Stats;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter.FilterProcess;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.job.MilkJob;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.job.MilkJobExecution.DelayResult;
import org.bonitasoft.truckmilk.job.MilkJobExecution.ListProcessesResult;

public class MilkSonar extends MilkPlugIn {

    private final static String CSTPROCESSVERSIONPOLICY_LAST = "Last version";
    private final static String CSTPROCESSVERSIONPOLICY_ALL = "All version";

    private final static String CSTPROCESSSTATUS_ENABLE = "Enable only";
    private final static String CSTPROCESSSTATUS_DISABLE = "Disable only";

    private final static String CSTPROCESSSTATUSPOLICY_ALL = "All version";

    private final static String CSTACTION_DONOTHING = "Do nothing";
    private final static String CSTACTION_DISABLE = "Disable it";

    private final static String CSTCONFIGURATION_DEFAULT = "Default";
    private final static String CSTCONFIGURATION_SONARPAGE = "Sonar page";

    
    private final static String CSTREPORTLEVEL_ALL = "All";
    private final static String CSTREPORTLEVEL_INFO = "Info, Tips, Warning, Severe";
    private final static String CSTREPORTLEVEL_WARNING= "Warning, Severe";
    private final static String CSTREPORTLEVEL_SEVERE = "Severe";
    
    

    private final static PlugInParameter cstParamProcessFilter = PlugInParameter.createInstance("processfilter", "Process Filter", TypeParameter.ARRAYPROCESSNAME, null, "Give a list of process name. Name must be exact, no version is given (all versions will be purged)")
            .withFilterProcess(FilterProcess.ALL);

    private final static PlugInParameter cstParamProcessVersionPolicy = PlugInParameter.createInstanceListValues("processversionpolicy", "Process Version Policy",
            new String[] { CSTPROCESSVERSIONPOLICY_LAST, CSTPROCESSVERSIONPOLICY_ALL }, CSTPROCESSVERSIONPOLICY_LAST, "Manage all processes, or only the last version (last deployed)");
    private final static PlugInParameter cstParamProcessStatusPolicy = PlugInParameter.createInstanceListValues("processstatuspolicy", "Process Status Policy",
            new String[] { CSTPROCESSSTATUS_ENABLE, CSTPROCESSSTATUS_DISABLE, CSTPROCESSSTATUSPOLICY_ALL }, CSTPROCESSSTATUS_ENABLE, "Manage all processes, or only ENABLE, DISABLE process");

    private final static PlugInParameter cstParamDelay = PlugInParameter.createInstanceDelay("delayinday", "Delay", DELAYSCOPE.WEEK, 1, "All processes deployed inside this delay are checked");

    private final static PlugInParameter cstParamAction = PlugInParameter.createInstanceListValues("action", "When a process has errors",
            new String[] { CSTACTION_DONOTHING, CSTACTION_DISABLE }, CSTACTION_DONOTHING, "When a process has error, it can be disabled");

    private final static PlugInParameter cstParamParameters = PlugInParameter.createInstanceListValues("parameters", "Parameters",
            new String[] { CSTCONFIGURATION_DEFAULT, CSTCONFIGURATION_SONARPAGE }, CSTCONFIGURATION_SONARPAGE, "Use the default parameter, or the one saved in the Sonar Page");

    private final static PlugInParameter cstParamQualification = PlugInParameter.createInstanceListValues("qualification", "Qualification",
            new String[] { CSTCONFIGURATION_DEFAULT, CSTCONFIGURATION_SONARPAGE }, CSTCONFIGURATION_SONARPAGE, "Use the default qualification, or the one saved in the Sonar Page");

    private final static PlugInParameter cstParamReportLevel = PlugInParameter.createInstanceListValues("reportlevel", "Report Level",
            new String[] { CSTREPORTLEVEL_ALL, CSTREPORTLEVEL_INFO, CSTREPORTLEVEL_WARNING, CSTREPORTLEVEL_SEVERE }, CSTREPORTLEVEL_ALL, "Select the level reported. If INFO, only INFO, TIP, WARNING, SEVERE are reported for example");

    private final static PlugInParameter cstParamCsvSeparator = PlugInParameter.createInstanceInformation( "csvseparator", "CsvSeparator", "The CSV Separator is defined in the Sonar Parameters. Access this page to change it");

    private final static PlugInParameter cstParamReport = PlugInParameter.createInstanceFile("report", "Report Sonar", TypeParameter.FILEWRITE, null, "Result of Sonar Analysis", "Sonar.zip", "application/CSV");

    private final static PlugInMeasurement cstMesureErrors = PlugInMeasurement.createInstance("Errors", "Number of errors", "Give the number of errors detected");
    private final static PlugInMeasurement cstMesureProcessErrors = PlugInMeasurement.createInstance("ProcessErrors", "Number of processes in errors", "Give the number of process in error detected (ERROR)");
    private final static PlugInMeasurement cstMesureProcessCorrect = PlugInMeasurement.createInstance("ProcessCorrects", "Number of processes correct", "Give the number of process correct detected (WARNING,INFO,TIPS)");
    private final static PlugInMeasurement cstMesureRatioCorrectProcess = PlugInMeasurement.createInstance("RatioProcessCorrect", "Ratio process correct", "Give the ratio (in %) of correct process");

    private final static String LOGGER_LABEL = "MilkSonar";
    private final static Logger logger = Logger.getLogger(MilkSonar.class.getName());

    private final static BEvent eventErrorExecution = new BEvent(MilkSonar.class.getName(), 1, Level.ERROR,
            "Error execution ", "Error during execution ", "Validation can't process", "Check exception");

    private final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

    public MilkSonar() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * plug in can check its environment, to detect if you missed something. An external component may
     * be required and are not installed.
     * 
     * @return a list of Events.
     */
    @Override
    public List<BEvent> checkPluginEnvironment(MilkJobExecution milkJobExecution) {
        return new ArrayList<>();
    }

    /**
     * check the Job's environment
     */
    public List<BEvent> checkJobEnvironment(MilkJobExecution milkJobExecution) {
        return new ArrayList<>();
    }

    /**
     * return the description of ping job
     */
    @Override
    public MilkPlugInDescription getDefinitionDescription(MilkJobContext milkJobContext) {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();

        plugInDescription.setName("Sonar");
        plugInDescription.setExplanation("Call a Sonar verification on processes");
        plugInDescription.setLabel("Sonar");

        plugInDescription.setCategory(CATEGORY.PROCESSES);
        plugInDescription.setStopJob(JOBSTOPPER.BOTH);

        plugInDescription.addParameter(cstParamProcessFilter);
        plugInDescription.addParameter(cstParamProcessVersionPolicy);
        plugInDescription.addParameter(cstParamProcessStatusPolicy);
        plugInDescription.addParameter(cstParamDelay);
        plugInDescription.addParameter(cstParamAction);
        plugInDescription.addParameter(cstParamParameters);
        plugInDescription.addParameter(cstParamQualification);
        plugInDescription.addParameter(cstParamReportLevel);        
        plugInDescription.addParameter(cstParamReport);
        plugInDescription.addParameter(cstParamCsvSeparator);

        plugInDescription.addMesure(cstMesureErrors);
        plugInDescription.addMesure(cstMesureProcessErrors);
        plugInDescription.addMesure(cstMesureProcessCorrect);
        plugInDescription.addMesure(cstMesureRatioCorrectProcess);

        return plugInDescription;
    }

    /**
     * execution of the job. Just calculated the result according the parameters, and return it.
     */
    @Override
    public MilkJobOutput executeJob(MilkJobExecution milkJobExecution) {
        MilkJobOutput milkJobOutput = milkJobExecution.getMilkJobOutput();

        Integer maximumArchiveDeletionPerRound = milkJobExecution.getJobStopAfterMaxItems();
        // default value is 1 Million
        if (maximumArchiveDeletionPerRound == null || maximumArchiveDeletionPerRound.equals(MilkJob.CSTDEFAULT_STOPAFTER_MAXITEMS))
            maximumArchiveDeletionPerRound = 1000000;

        SearchOptionsBuilder searchActBuilder = new SearchOptionsBuilder(0, (int) maximumArchiveDeletionPerRound + 1);
        milkJobOutput.setExecutionStatus( ExecutionStatus.SUCCESS );

        try {
            // ----------------------- Parameters
            // List of process ENABLE AND DISABLE
            ListProcessesResult listProcessResult =  milkJobExecution.getInputArrayProcess( cstParamProcessFilter, false, searchActBuilder, ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, milkJobExecution.getApiAccessor().getProcessAPI());

            if (BEventFactory.isError(listProcessResult.listEvents)) {
                // filter given, no process found : stop now
                milkJobOutput.addEvents(listProcessResult.listEvents);
                milkJobOutput.setExecutionStatus( ExecutionStatus.BADCONFIGURATION );
                return milkJobOutput;
            }

            String processVersionPolicy = milkJobExecution.getInputStringParameter(cstParamProcessVersionPolicy);
            String processStatusPolicy = milkJobExecution.getInputStringParameter(cstParamProcessStatusPolicy);

            if (CSTPROCESSSTATUS_ENABLE.equals(processStatusPolicy))
                listProcessResult.sob.filter(ProcessDeploymentInfoSearchDescriptor.ACTIVATION_STATE, ActivationState.ENABLED.toString());
            else if (CSTPROCESSSTATUS_DISABLE.equals(processStatusPolicy))
                listProcessResult.sob.filter(ProcessDeploymentInfoSearchDescriptor.ACTIVATION_STATE, ActivationState.DISABLED.toString());

            // if the date has to be added in the result ?
            DelayResult delayResult = milkJobExecution.getInputDelayParameter( cstParamDelay, new Date(), false);
            if (BEventFactory.isError(delayResult.listEvents)) {
                milkJobOutput.addEvents(delayResult.listEvents);
                milkJobOutput.setExecutionStatus( ExecutionStatus.ERROR );
                return milkJobOutput;
            }
            if (delayResult.delayInMs > 0)
                listProcessResult.sob.greaterThan(ProcessDeploymentInfoSearchDescriptor.DEPLOYMENT_DATE, delayResult.delayDate.getTime());

            String action = milkJobExecution.getInputStringParameter(cstParamProcessStatusPolicy);


            /* Configuration */
            SonarConfiguration sonarConfiguration = new SonarConfiguration(milkJobExecution.getApiAccessor().getProcessAPI(), milkJobExecution.getTenantId());
            TenantServiceAccessor tenantServiceAccessor = milkJobExecution.getMilkJobContext().getTenantServiceAccessor();
            if (tenantServiceAccessor != null) {
                ClassLoader classLoader = tenantServiceAccessor.getClassLoaderService().getGlobalClassLoader();
                sonarConfiguration.setClassLoader(classLoader);
            }

            List<BEvent> listEvents = new ArrayList<>();

            String parameters = milkJobExecution.getInputStringParameter(cstParamParameters);
            if (CSTCONFIGURATION_SONARPAGE.equals(parameters)) {
                listEvents.addAll(sonarConfiguration.readRulesParameters());
            }

            String qualification = milkJobExecution.getInputStringParameter(cstParamQualification);
            if (CSTCONFIGURATION_SONARPAGE.equals(qualification)) {
                listEvents.addAll(sonarConfiguration.readQualification());
            }
            if (BEventFactory.isError(listEvents)) {
                milkJobOutput.addEvents(listEvents);
                milkJobOutput.setExecutionStatus( ExecutionStatus.BADCONFIGURATION );
                return milkJobOutput;
            }

            String reportLevel = milkJobExecution.getInputStringParameter( cstParamReportLevel );
            SEVERITY severity = SEVERITY.SUCCESS;
            if (CSTREPORTLEVEL_INFO.equals(reportLevel))
                severity = SEVERITY.INFO;
            if (CSTREPORTLEVEL_SEVERE.equals(reportLevel))
                severity = SEVERITY.SEVERE;
            if (CSTREPORTLEVEL_WARNING.equals(reportLevel))
                severity = SEVERITY.WARNING;
                
            // ------------------- search

            SearchResult<ProcessDeploymentInfo> searchProcess;

            Set<String> processName = new HashSet<>();
            searchProcess = milkJobExecution.getApiAccessor().getProcessAPI().searchProcessDeploymentInfos(listProcessResult.sob.done());

            MilkSonarTrackExecution milkSonarTrackExecution = new MilkSonarTrackExecution(milkJobExecution);
            milkSonarTrackExecution.init(searchProcess.getResult().size());

            SonarEngine sonarEngine = SonarEngine.getInstance();
            SonarResult sonarResult = new SonarResult();
            // ----------------------- loop on each process
            for (ProcessDeploymentInfo processDeploymentInfo : searchProcess.getResult()) {
                if (milkJobExecution.isStopRequired())
                    break;

                // register the process name. We ordered by Deployment date, so if the policy is "older" and the name is already register, skip it
                if (CSTPROCESSVERSIONPOLICY_LAST.equals(processVersionPolicy)) {
                    if (processName.contains(processDeploymentInfo.getName())) {
                        milkSonarTrackExecution.startProcess(0, processDeploymentInfo);
                        milkSonarTrackExecution.endProcess();

                        continue;
                    }
                }
                processName.add(processDeploymentInfo.getName());

                SonarProcess sonarProcess = new SonarProcess(milkJobExecution.getApiAccessor().getProcessAPI(), milkJobExecution.getTenantId());
                listEvents = sonarProcess.loadProcess(processDeploymentInfo.getProcessId());
                milkJobOutput.addEvents(listEvents);
                if (BEventFactory.isError(listEvents)) {
                    milkJobOutput.addEvents(listEvents);
                    continue;
                }
                SonarResult resultProcess = sonarEngine.executeDetection(sonarProcess, sonarConfiguration, milkSonarTrackExecution);
                sonarResult.add(resultProcess);
                SonarResultProcess sonarResultProcess = resultProcess.getSonarResultProcess(sonarProcess);

                if (sonarResultProcess.isError()) {
                    if (CSTACTION_DISABLE.equals(action)) {
                        try {
                            milkJobExecution.getApiAccessor().getProcessAPI().disableProcess(sonarProcess.getProcessDefinitionId());
                            sonarResultProcess.actionProcessDisabled = true;
                        } catch (ProcessDefinitionNotFoundException | ProcessActivationException e) {
                            // nothing special here
                        }
                    }
                }

                milkJobExecution.addManagedItems(1);
                milkJobOutput.nbItemsProcessed++;
            } // end for
            
            SonarResultDecoHtml decoHtml = new SonarResultDecoHtml();
            StringBuffer html = new StringBuffer();
            decoHtml.getHtml(sonarResult, severity, html);
            milkJobOutput.addReportInHtml(html.toString());

            SonarResultDecoCsv decoCsv = new SonarResultDecoCsv();
            ByteArrayOutputStream csvContent = new ByteArrayOutputStream();
            decoCsv.getCSVZip(sonarResult, severity, csvContent, sonarConfiguration.getCsvSeparator());
            milkJobOutput.setParameterStream(cstParamReport, new ByteArrayInputStream(csvContent.toByteArray()));

            SonarResultDecoStatistics decoStats = new SonarResultDecoStatistics();
            Stats stats = decoStats.getStats(sonarResult);

            milkJobOutput.setMeasure(cstMesureErrors, stats.totalErrors);
            milkJobOutput.setMeasure(cstMesureProcessErrors, stats.totalErrorProcess);
            milkJobOutput.setMeasure(cstMesureProcessCorrect, stats.totalCorrectProcess);
            if (stats.totalCorrectProcess + stats.totalErrorProcess > 0)
                milkJobOutput.setMeasure(cstMesureRatioCorrectProcess, (int) (100.0 * stats.totalCorrectProcess / (1.0 + stats.totalCorrectProcess + stats.totalErrorProcess)));
            else
                milkJobOutput.setMeasure(cstMesureRatioCorrectProcess, 0);

            if (stats.totalErrorProcess > 0 )
                milkJobOutput.setExecutionStatus( ExecutionStatus.ERROR );

        } catch (Exception e) {
            milkJobOutput.addEvent(new BEvent(eventErrorExecution, e, "During execution"));
            milkJobOutput.setExecutionStatus( ExecutionStatus.ERROR );
        }
        milkJobOutput.addChronometersInReport(false, true);

        // it maybe already in error, so don't change it
        if (milkJobExecution.isStopRequired() && milkJobOutput.getExecutionStatus() == ExecutionStatus.SUCCESS)
            milkJobOutput.setExecutionStatus( ExecutionStatus.SUCCESSPARTIAL );

        return milkJobOutput;
    }

    /**
     * @author Firstname Lastname
     */
    private final class MilkSonarTrackExecution extends SonarTrackExecution {

        MilkJobExecution jobExecution;

        MilkSonarTrackExecution(MilkJobExecution jobExecution) {
            this.jobExecution = jobExecution;

        }

        @Override
        public void startProcess(int numberOfRulesInTheProcess, ProcessDeploymentInfo processDeploymentInfo) {
            super.startProcess(numberOfRulesInTheProcess, processDeploymentInfo);
            jobExecution.setAvancement(percentageAdvanceProcess);
            jobExecution.setAvancementInformation("Process [" + processDeploymentInfo.getName() + " (" + processDeploymentInfo.getVersion() + ")]");
        }

        @Override
        public void endProcess() {
            super.endProcess();
            jobExecution.setAvancement(percentageAdvanceProcess);
        }

        @Override
        public void startRule(SonarRule sonarRule) {
            super.startRule(sonarRule);
            jobExecution.setAvancement(percentageAdvanceProcess);
            jobExecution.setAvancementInformation("Process [" + processDeploymentInfo.getName() + " (" + processDeploymentInfo.getVersion() + ")] Rule [" + sonarRule.getName() + "]");
        }

        @Override
        public void endRule() {
            super.endRule();
            jobExecution.setAvancement(percentageAdvanceProcess);
        }
    }
}
