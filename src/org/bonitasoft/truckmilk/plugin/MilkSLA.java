package org.bonitasoft.truckmilk.plugin;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.api.IdentityAPI;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.actor.ActorInstance;
import org.bonitasoft.engine.bpm.actor.ActorNotFoundException;
import org.bonitasoft.engine.bpm.data.DataInstance;
import org.bonitasoft.engine.bpm.data.DataNotFoundException;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstance;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstanceSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfo;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfoSearchDescriptor;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.exception.UpdateException;
import org.bonitasoft.engine.identity.ContactData;
import org.bonitasoft.engine.identity.User;
import org.bonitasoft.engine.identity.UserNotFoundException;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.MilkLog;
import org.bonitasoft.truckmilk.toolbox.PlaceHolder;
import org.bonitasoft.truckmilk.toolbox.SendMail;
import org.bonitasoft.truckmilk.toolbox.SendMailEnvironment;
import org.bonitasoft.truckmilk.toolbox.SendMailParameters;
import org.bonitasoft.truckmilk.toolbox.TypesCast;

public class MilkSLA extends MilkPlugIn {

    static MilkLog logger = MilkLog.getLogger(MilkSLA.class.getName());

    private static BEvent EVENT_NO_PROCESS_MATCH_FILTER = new BEvent(MilkPurgeArchive.class.getName(), 1,
            Level.APPLICATIONERROR,
            "No process match filter", "No process is found with the given filter", "This filter does not apply.",
            "Check the process name");

    private static BEvent EVENT_VARIABLE_REGISTRATION_NOT_FOUND = new BEvent(MilkSLA.class.getName(), 2,
            Level.APPLICATIONERROR,
            "Variable not found", "The variable registration referenced in the rule does not exist in this process", "Impossible to declare the rule was executed or not. SLA considere this is already executed then.",
            "Add the process variable as a MAP, else use the delaiRegistration");

    private static BEvent EVENT_NO_MECHANISM_REGISTER = new BEvent(MilkSLA.class.getName(), 3,
            Level.APPLICATIONERROR,
            "No mechanism registerd", "To detect if a rule is already executed or not, two mechanism is possible, by a variable or a Duration slot. No mechanism is selected.", "Impossible to declare the rule was executed or not. SLA considere this is already executed then.",
            "Choose a mechanism");

    private static BEvent EVENT_VARIABLE_REGISTRATON_CLASS_ERROR = new BEvent(MilkSLA.class.getName(), 4,
            Level.APPLICATIONERROR,
            "Variable Class Error", "The variable registration referenced in the rule must be a Map", "Impossible to declare the rule was executed or not. SLA considere this is already executed then.",
            "Reference this variable as a HASHMAP");

    private static BEvent EVENT_VARIABLE_UPDATE_FAILED = new BEvent(MilkSLA.class.getName(), 5,
            Level.APPLICATIONERROR,
            "Update Variable failed", "After the execution, the variable must be updated to not reexecute the same SLA. Update failed", "The task will be reexecuted.",
            "Check the error");

    private static BEvent EVENT_UNKNOW_ACTION = new BEvent(MilkSLA.class.getName(), 6,
            Level.APPLICATIONERROR,
            "Unknow Action",
            "Action unknown",
            "Give an existing action.",
            "Check the action of the rule");

    private static BEvent EVENT_ASSIGN_TASK_FAILED = new BEvent(MilkSLA.class.getName(), 7,
            Level.APPLICATIONERROR,
            "Assignement failed",
            "It is not possible to assign a task to a user",
            "Task is not assigned",
            "Check the error of assignement");

    private static BEvent EVENT_USER_NOT_FOUND = new BEvent(MilkSLA.class.getName(), 7,
            Level.APPLICATIONERROR,
            "User not found",
            "The requested user is not found",
            "No action done",
            "Check the user name in your organization");

    private static BEvent EVENT_NO_USER = new BEvent(MilkSLA.class.getName(), 8,
            Level.APPLICATIONERROR,
            "No user found for this task",
            "This task is not pending or assign to any user",
            "Operation faiked",
            "Check the tasks");

    private static BEvent EVENT_ACTOR_NOT_FOUND = new BEvent(MilkSLA.class.getName(), 9,
            Level.APPLICATIONERROR,
            "No actor in the tasks",
            "This task does not have an actor",
            "Operation faiked",
            "Check the tasks");

    private static BEvent EVENT_SLA_EXECUTION_DONE = new BEvent(MilkSLA.class.getName(), 10,
            Level.SUCCESS,
            "SLA rules executed",
            "A set of execution rule was performed");

    private static BEvent EVENT_CASEID_NOT_SET = new BEvent(MilkSLA.class.getName(), 10,
            Level.APPLICATIONERROR,
            "CaseId not set",
            "Give a correct CASEID for the analysis", "No analysis is performed", "Case Id is a number");

    private static BEvent EVENT_SLA_ANALYSIS = new BEvent(MilkSLA.class.getName(), 11,
            Level.SUCCESS,
            "Analysis is done",
            "Please find the analysis");

    private static BEvent EVENT_SLA_ERROR = new BEvent(MilkSLA.class.getName(), 12,
            Level.ERROR,
            "Error during execution",
            "An error arrived during the execution", "Execution stopped", "Contact the adminstrator");

    private static PlugInParameter cstParamProcessName = PlugInParameter.createInstance("ProcessName", "Process Name", TypeParameter.PROCESSNAME, "", "Process name is mandatory. You can specify the process AND the version, or only the process name: all versions of this process is then checked");

    private static PlugInParameter cstParamRuleSLA = PlugInParameter.createInstanceArrayMap("RuleSLA", "Rules SLA",
            Arrays.asList(
                    ColDefinition.getInstance("TASKNAME", "TaskName", "Task name in a process. Multiple tasks may be reference one time using # like 'Validate#Review#Control", TypeParameter.STRING, 50),
                    ColDefinition.getInstance("PERCENT", "Percent Threashold", "0%: task creation, 100%=due date", TypeParameter.LONG, 20),
                    ColDefinition.getInstance("ACTION", "Action", ACTION.EMAILUSER.toString() + ":<userName>, "
                            + ACTION.EMAILCANDIDATES.toString() + ", "
                            + ACTION.EMAILACTOR.toString() + ":<actor>,"
                            + ACTION.ASSIGNUSER.toString() + ":<userName>, "
                            + ACTION.ASSIGNSUPERVISOR.toString() + ":[1] ", TypeParameter.STRING, 50)),
            null, "Give a list of rules. Each rule describe the threshold in percent 0-task start, 100% due date, and action");
    private static PlugInParameter cstParamEmailFrom = PlugInParameter.createInstance("emailfrom", "Email from", TypeParameter.STRING, "bonitasoftreminder@myserver.com", "The 'mail from' attribute");
    private static PlugInParameter cstParamEmailSubject = PlugInParameter.createInstance("EmailSubject", "Email subject", TypeParameter.STRING, "The task {{taskName}} in case {{caseId}} need your attention", "Subject of the mail. See the content to the list of Place holder allowed");
    private static PlugInParameter cstParamEmailContent = PlugInParameter.createInstance("EmailContent", "Email content", TypeParameter.TEXT, "A task is close to its due date. Access the {{taskUrl}}",
            "Content of the mail. Place holder are <i>taskName</i>, <i>caseId</i>, <i>percentThreashold</i>% threasold active, <i>userFirstName</i>User first name, receiver of the mail, <i>userLastName</i>User last name, receiver of the mail, <i>userTitle</i>User title, receiver of the mail");
    private static PlugInParameter cstParamEmailBonitaServer = PlugInParameter.createInstance("EmailBonitaServer", "Bonita server", TypeParameter.STRING, "http://localhost:8080", "HTTP address to join the caseId, if you place the <i>taskURL</i> in the content on the mail, to access the Bonita server");
    private static PlugInParameter cstParamProcessVariableRegistration = PlugInParameter.createInstance("ProcessVariableRegistration", "Process Variable registration", TypeParameter.STRING, null, "To avoid to execute the same rule, give access to a Process Variable HASHMAP, then all executions are registered on this data");
    private static PlugInParameter cstParamDelaiRegistrationInMn = PlugInParameter.createInstance("DelaiRegistrationInMinute", "Delay registration in minutes", TypeParameter.LONG, null,
            "To avoid to execute the same rule, and you can't give a Process variable, allow a registration in minute. Action is executed if the time slot is 'Fire < Current time < File + Duration'");
    private static PlugInParameter cstParamMaximumTask = PlugInParameter.createInstance("MaximumTask", "Maximum tasks", TypeParameter.LONG, 10000L, "In order to protect the server, this is the maxim task the rule will check in the process");

    public static PlugInParameter cstParamAnalyseCaseId = PlugInParameter.createInstanceButton("AnalyseCaseId", "Analyse a specific case", Arrays.asList("CaseId"), Arrays.asList(""), "Give a caseId and an explanation will be return to describe what was done for this caseId", "Click on the button to start the analysis");

    /**
     * 
     * 
     */
    public static enum ACTION {
        EMAILUSER, EMAILCANDIDATES, EMAILACTOR, ASSIGNUSER, ASSIGNSUPERVISOR
    }

    public MilkSLA() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Rule */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    private class Rule {

        public String taskName;
        public Set<String> listTasksName = null;
        public int percentThreashold;
        public String action;

        public Rule(Map<String, Object> ruleMap) {
            this.taskName = TypesCast.getString(ruleMap.get("TASKNAME"), null);
            if (this.taskName != null && this.taskName.trim().length() == 0)
                this.taskName = null;
            this.percentThreashold = TypesCast.getInteger(ruleMap.get("PERCENT"), 0);
            this.action = TypesCast.getString(ruleMap.get("ACTION"), "");
            if (this.action == null)
                this.action = "";
        }

        public ACTION getAction() {
            if (action.startsWith(ACTION.EMAILUSER.toString()))
                return ACTION.EMAILUSER;
            if (action.startsWith(ACTION.EMAILCANDIDATES.toString()))
                return ACTION.EMAILCANDIDATES;
            if (action.startsWith(ACTION.EMAILACTOR.toString()))
                return ACTION.EMAILACTOR;
            if (action.startsWith(ACTION.ASSIGNUSER.toString()))
                return ACTION.ASSIGNUSER;
            if (action.startsWith(ACTION.ASSIGNSUPERVISOR.toString()))
                return ACTION.ASSIGNSUPERVISOR;
            return null;
        }

        public String getActionParameters() {
            int posIndex = action.indexOf(":");
            if (posIndex > 0)
                return action.substring(posIndex + 1);
            return "";
        }

        public Set<String> getListTaskNames() {
            if (listTasksName != null)
                return listTasksName;
            listTasksName = new HashSet<String>();
            if (taskName != null) {
                StringTokenizer st = new StringTokenizer(taskName, "#");
                while (st.hasMoreTokens()) {
                    listTasksName.add(st.nextToken());
                }
            }
            return listTasksName;
        }

        public boolean isAllTasks() {
            return taskName == null;
        }

        public String getUniqueId() {
            return taskName + "#" + percentThreashold;
        }
    }

    private static class ExceptionWithEvents extends Exception {

        private static final long serialVersionUID = 2538564973248284226L;
        @SuppressWarnings("unused")
        public Exception e;
        public List<BEvent> listEvents = new ArrayList<BEvent>();
    }

    /**
     * it's embedded
     */
    public boolean isEmbeded() {
        return true;
    };

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Check Environnement */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    /**
     * check the environment : for the milkEmailUsersTasks, we require to be able to send an email
     */
    public List<BEvent> checkPluginEnvironment(long tenantId, APIAccessor apiAccessor) {
        return SendMailEnvironment.checkEnvironment(tenantId, this);
    };
    /**
     * check the Job's environment
     */
    public List<BEvent> checkJobEnvironment(MilkJobExecution jobExecution, APIAccessor apiAccessor) {
        List<BEvent> listEvents = new ArrayList<BEvent>();
        return listEvents;
    };

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Override abstract method */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    @Override
    public PlugTourOutput execute(MilkJobExecution input, APIAccessor apiAccessor) {

        PlugTourOutput plugTourOutput = executeSLA(input, null, apiAccessor);
        plugTourOutput.executionStatus = ExecutionStatus.SUCCESS;
        return plugTourOutput;
    }

    @Override
    public PlugInDescription getDefinitionDescription() {
        PlugInDescription plugInDescription = new PlugInDescription();
        plugInDescription.name = "SLA";
        plugInDescription.label = "SLA";
        plugInDescription.explanation = "Verify the SLA on human task, then notify if we reach some level like 75% of the task duration, of 100%. Can re-affect the task to a different user if needed.<br>";
        plugInDescription.explanation += " <i>ProcessName</i> : if empty, then rule apply for all processes<br>";
        plugInDescription.explanation += " <i>Rule SLA</i><br> ";
        plugInDescription.explanation += " * TASKNAME is the task where the rule apply (empty means all human task of the process)<br>";
        plugInDescription.explanation += " * PERCENT: 0 means the begining of the task. Else, calculation is perform between the start (0%) to the Due Date (100%). Over 100% is acceptable.<br>";
        plugInDescription.explanation += " * ACTION: (email use professionnal email)<br>";
        plugInDescription.explanation += " <ul>";
        plugInDescription.explanation += " <li> " + ACTION.EMAILUSER.name() + ":<userName></li>";
        plugInDescription.explanation += " <li> " + ACTION.EMAILACTOR.name() + ":<actor></li>";
        plugInDescription.explanation += " <li> " + ACTION.EMAILCANDIDATES.name() + "</li>";
        plugInDescription.explanation += " <li> " + ACTION.ASSIGNUSER.name() + ":<userName> directly assign the task to a user. </li>";
        plugInDescription.explanation += " <li> " + ACTION.ASSIGNSUPERVISOR.name() + ":[1] assign to supervisor (1) or supervisor of supervisor(2) - stop if no supervisor.";
        plugInDescription.explanation += " </ul><br>";
        plugInDescription.explanation += "<i>Registration</i><br>";
        plugInDescription.explanation += "To avoid to send twice a SLA email, you can setup a variable in the process. Job will register that it already sent the '60% milestone' email. Else, give a delay in minutes: if the milstone was between [CurrentTime - Delay, CurrentTime], then it is not resend";

        plugInDescription.addParameter(cstParamProcessName);
        plugInDescription.addParameter(cstParamRuleSLA);
        plugInDescription.addParameter(cstParamEmailFrom);
        plugInDescription.addParameter(cstParamEmailSubject);
        plugInDescription.addParameter(cstParamEmailContent);
        plugInDescription.addParameter(cstParamEmailBonitaServer);
        plugInDescription.addParameter(cstParamProcessVariableRegistration);
        plugInDescription.addParameter(cstParamDelaiRegistrationInMn);
        plugInDescription.addParameter(cstParamMaximumTask);
        plugInDescription.addParameter(cstParamAnalyseCaseId);

        try {
            SendMailParameters.addPlugInParameter(SendMailParameters.MAIL_DIRECTION.SENDONLY, plugInDescription);
        } catch (Error er) {
            // do nothing here, the error will show up again in the check Environment
            // Cause : the Email Jar file is not installed, then java.lang.NoClassDefFoundError: javax/mail/Address
        }

        /*
         * plugInDescription.addParameterFromMapJson(
         * "{\"delayinmn\":10,\"maxtentative\":12,\"processfilter\":[]}");
         */
        return plugInDescription;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Plugin parameters */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    @Override
    public List<BEvent> buttonParameters(String buttonName, MilkJobExecution input, Map<String, Object> argsParameters, APIAccessor apiAccessor) {
        if (buttonName.equals(SendMailParameters.cstParamTestSendMail.name)) {
            SendMail sendMail = new SendMail(input);
            return sendMail.testEmail(argsParameters);
        }
        if (buttonName.equals(cstParamAnalyseCaseId.name)) {
            Long caseId = TypesCast.getLong(argsParameters.get("CaseId"), null);
            if (caseId == null) {
                List<BEvent> listEvents = new ArrayList<BEvent>();
                listEvents.add(EVENT_CASEID_NOT_SET);
                return listEvents;
            }
            PlugTourOutput plugTourOutput = executeSLA(input, caseId, apiAccessor);
            return plugTourOutput.getListEvents();
        }
        // unkonw test
        List<BEvent> listEvents = new ArrayList<BEvent>();
        listEvents.add(new BEvent(MilkPlugIn.EVENT_UNKNOW_BUTTON, "Button [" + buttonName + "]"));
        return new ArrayList<BEvent>();
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Internal method */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    public class CollectAnalysisExecution {

        public boolean isActive = false;
        public Long caseId;
        public String explanation;
        public boolean recordIsOn = false;

        public void addExplanation(boolean forceRecord, String comment) {
            if (forceRecord || recordIsOn)
                explanation += comment + ";";
        }
    }

    public PlugTourOutput executeSLA(MilkJobExecution input, Long analysisCaseId, APIAccessor apiAccessor) {
        CollectAnalysisExecution collectAnalysisExecution = new CollectAnalysisExecution();
        collectAnalysisExecution.caseId = analysisCaseId;
        collectAnalysisExecution.isActive = analysisCaseId != null;

        PlugTourOutput plugTourOutput = input.getPlugTourOutput();
        ProcessAPI processAPI = apiAccessor.getProcessAPI();
        IdentityAPI identityAPI = apiAccessor.getIdentityAPI();
        // get Input 
        String processName = input.getInputStringParameter(cstParamProcessName);
        List<Map<String, Object>> listRuleSLA = input.getInputListMapParameter(cstParamRuleSLA);
        long maxTasks = input.getInputLongParameter(cstParamMaximumTask);
        int numberOfExecution = 0;
        int numberOfAnalysis = 0;
        String detailsExecution = "";
        try {
            SearchResult<ProcessDeploymentInfo> searchProcessInfo = getListProcessDefinitionId(processName, processAPI);
            if (searchProcessInfo.getCount() == 0)
                plugTourOutput.addEvent(new BEvent(EVENT_NO_PROCESS_MATCH_FILTER, "ProcessName[" + processName + "]"));

            for (ProcessDeploymentInfo processInfo : searchProcessInfo.getResult()) {

                // search all tasks
                boolean searchOnAllTasks = false;
                collectAnalysisExecution.addExplanation(true, "CheckProcess [" + processInfo.getName() + "/" + processInfo.getVersion() + "] processId[" + processInfo.getProcessId() + "]");

                List<String> setTasks = new ArrayList<String>();
                for (Map<String, Object> oneRule : listRuleSLA) {
                    Rule rule = new Rule(oneRule);
                    if (rule.isAllTasks())
                        // special case : mean "all tasks on the process
                        searchOnAllTasks = true;
                    else
                        for (String taskName : rule.getListTaskNames()) {
                            if (!setTasks.contains(taskName))
                                setTasks.add(taskName);
                        }
                }
                boolean executeOneTime = false;
                if (searchOnAllTasks) {
                    setTasks.clear();
                    executeOneTime = true;
                }
                collectAnalysisExecution.addExplanation(true, " FilterOnTask? All=[" + searchOnAllTasks + "]");
                if (!searchOnAllTasks)
                    collectAnalysisExecution.addExplanation(true, "-onlyOnTask:" + setTasks);

                // don't play one request per rule : maybe too expensive. So, play one big request for all rules
                // probleme : if we have too much task name, request may be too long (number of char limited), so do the request per packet of tasks
                int pageIndexTask = 0;
                while (executeOneTime || (pageIndexTask < setTasks.size())) {
                    executeOneTime = false;
                    // run 20 task
                    // search all active human task according the rule - we don't want a big request, so we must truncate per 
                    SearchOptionsBuilder searchOption = new SearchOptionsBuilder((int) (pageIndexTask * maxTasks), (int) maxTasks);
                    searchOption.filter(HumanTaskInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processInfo.getProcessId());
                    if (!searchOnAllTasks) {
                        searchOption.leftParenthesis();
                        for (int i = 0; i < 20; i++) {
                            if (i + pageIndexTask >= setTasks.size())
                                break;
                            if (i > 0)
                                searchOption.or();
                            searchOption.filter(HumanTaskInstanceSearchDescriptor.NAME, setTasks.get(i + pageIndexTask));
                        }
                        pageIndexTask += 20;
                        searchOption.rightParenthesis();
                    } else
                        // no filter on task : we search all, so no pagination need
                        pageIndexTask = setTasks.size() + 1;

                    SearchResult<HumanTaskInstance> searchTasks = processAPI.searchHumanTaskInstances(searchOption.done());

                    collectAnalysisExecution.addExplanation(true, "SearchInPage [" + (pageIndexTask * maxTasks) + " , " + maxTasks + "] - result=" + searchTasks.getCount());

                    for (HumanTaskInstance humanTask : searchTasks.getResult()) {
                        if (collectAnalysisExecution.caseId != null && humanTask.getParentProcessInstanceId() == collectAnalysisExecution.caseId) {
                            collectAnalysisExecution.addExplanation(true, "CASE DETECTED TaskId[" + humanTask.getId() + "]");
                            collectAnalysisExecution.recordIsOn = true;
                        } else
                            collectAnalysisExecution.recordIsOn = false;

                        // is this human task fit a rule ?
                        for (Map<String, Object> ruleMap : listRuleSLA) {
                            Rule rule = new Rule(ruleMap);

                            // rule.rule.taskName can contains a # to give the same rule for multiple task
                            if (rule.isAllTasks() || rule.getListTaskNames().contains(humanTask.getName())) {
                                numberOfAnalysis++;
                                collectAnalysisExecution.addExplanation(false, " ****** ANALYSIS RULE PercentThreashold[" + rule.percentThreashold + "] **");
                                // ok, this rule apply.
                                try {
                                    if (!isRuleActive(humanTask, rule, input, processAPI, collectAnalysisExecution)) {
                                        collectAnalysisExecution.addExplanation(false, "~~~> Rule Not active  ******");
                                        continue;
                                    }
                                    // ok, apply if    
                                    numberOfExecution++;
                                    if (collectAnalysisExecution.isActive)
                                        collectAnalysisExecution.addExplanation(false, "~~~> Rule READY TO BE EXECUTED  ******");
                                    else {
                                        detailsExecution += humanTask.getRootContainerId() + ",";
                                        List<BEvent> listEvents = executeRule(humanTask, rule, input, processAPI, identityAPI);
                                        plugTourOutput.addEvents(listEvents);
                                        updateCase(humanTask, rule, input, processAPI);
                                    }
                                } catch (ExceptionWithEvents e) {
                                    plugTourOutput.addEvents(e.listEvents);
                                }
                            }
                        }
                    }
                } // end page of Task to apply

            }
            if (!collectAnalysisExecution.isActive) {
                if (numberOfExecution == 0) {
                    plugTourOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
                    plugTourOutput.addEvent(new BEvent(EVENT_SLA_EXECUTION_DONE, "Analysis:" + numberOfAnalysis));
                } else
                    plugTourOutput.addEvent(new BEvent(EVENT_SLA_EXECUTION_DONE, "On cases:" + detailsExecution + " Analysis:" + numberOfAnalysis));

                if (plugTourOutput.getListEvents() != null && BEventFactory.isError(plugTourOutput.getListEvents()))
                    plugTourOutput.executionStatus = ExecutionStatus.ERROR;
            }
        } catch (Exception e1) {
            StringWriter sw = new StringWriter();
            e1.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe("MilkSLA: ~~~~~~~~~~  : ERROR " + e1 + " at " + exceptionDetails);
            plugTourOutput.addEvent(new BEvent(EVENT_SLA_ERROR, e1, ""));

            plugTourOutput.executionStatus = ExecutionStatus.ERROR;
        }

        if (analysisCaseId != null) {
            plugTourOutput.addEvent(new BEvent(EVENT_SLA_ANALYSIS, collectAnalysisExecution.explanation));
        }
        return plugTourOutput;
    }

    /**
     * retrieve the list of ProcessName from the filter
     * search all process with the name (we may have multiple version)
     * format is "PROCESSNAME (VERSION)" or "PROCESSNAME"
     * 
     * @param processName
     * @return
     * @throws SearchException
     */
    private SearchResult<ProcessDeploymentInfo> getListProcessDefinitionId(String processName, ProcessAPI processAPI) throws SearchException {

        processName = processName.trim();
        String processNameOnly = processName;
        String processVersionOnly = null;
        if (processName.endsWith(")")) {
            int firstParenthesis = processName.lastIndexOf("(");
            if (firstParenthesis != -1) {
                processNameOnly = processName.substring(0, firstParenthesis - 1);
                processVersionOnly = processName.substring(firstParenthesis + 1, processName.length() - 1);
            }
        }
        SearchOptionsBuilder searchOption = new SearchOptionsBuilder(0, 1000);
        searchOption.filter(ProcessDeploymentInfoSearchDescriptor.NAME, processNameOnly);
        if (processVersionOnly != null) {
            searchOption.filter(ProcessDeploymentInfoSearchDescriptor.VERSION, processVersionOnly);
        }
        return processAPI.searchProcessDeploymentInfos(searchOption.done());

    }

    /**
     * is the rule is active for this task ?
     * 
     * @param humanTask the task
     * @param rule the rule to check
     * @param processAPI
     * @return
     */

    private boolean isRuleActive(HumanTaskInstance humanTask, Rule rule, MilkJobExecution input, ProcessAPI processAPI, CollectAnalysisExecution collectAnalysisExecution) throws ExceptionWithEvents {

        Date startDate = humanTask.getReachedStateDate();
        if (rule.percentThreashold == 0) {
            collectAnalysisExecution.addExplanation(false, "no threashold;");
            // this is the started rule. If the rule is not yet executed, do it
            if (isAlreadyExecutedRule(humanTask, startDate.getTime(), rule, input, processAPI, collectAnalysisExecution)) {
                return false;
            }
            return true;
        }
        Date dueDate = humanTask.getExpectedEndDate();
        if (dueDate == null) {
            collectAnalysisExecution.addExplanation(false, "No EndDate set in this task");
            return false;
        }
        // calculated the percentage of advancement for this task
        // startDate =0%
        // dueDate = 100%
        long currentTime = System.currentTimeMillis();
        long relativeTime = currentTime - startDate.getTime();
        long delayExpected = dueDate.getTime() - startDate.getTime();
        int percent = (int) ((double) 100.0 * relativeTime / delayExpected);
        if (percent <= rule.percentThreashold) {
            collectAnalysisExecution.addExplanation(false, "NOT YET: task_percent[" + percent + "]/rule.Percent[" + rule.percentThreashold + "]");
            return false;
        }

        // question : when the percentage threashold was supposed to reach ? because if we are in "time slot", this is the real moment we were suppose to start the action
        long delayAction = delayExpected * rule.percentThreashold / 100;
        long realAction = startDate.getTime() + delayAction;

        if (isAlreadyExecutedRule(humanTask, realAction, rule, input, processAPI, collectAnalysisExecution)) {
            collectAnalysisExecution.addExplanation(false, "already executed");
            return false;
        }
        collectAnalysisExecution.addExplanation(false, "not already executed: DO IT");
        return true;
    }

    /**
     * is this rule was already executed ?
     * Two way to know that:
     * - cstParamProcessVariableRegistration ? Then access this variable, which is suppose to be a
     * MAP, and then register the activity
     * - cstParamDelaiRegistration ? then we have to be in this delay
     * 
     * @param humanTask
     * @param rule
     * @param processAPI
     * @return
     */

    private boolean isAlreadyExecutedRule(HumanTaskInstance humanTask, long timeToExecute, Rule rule, MilkJobExecution input, ProcessAPI processAPI, CollectAnalysisExecution collectAnalysisExecution) throws ExceptionWithEvents {
        String variableName = input.getInputStringParameter(cstParamProcessVariableRegistration);
        Long delaiRegistrationInMn = input.getInputLongParameter(cstParamDelaiRegistrationInMn);
        DataInstance data = null;
        try {
            if (variableName != null && variableName.trim().length() > 0) {
                // based on a variable
                data = processAPI.getProcessDataInstance(variableName, humanTask.getParentProcessInstanceId());
                @SuppressWarnings("unchecked")
                Map<String, Object> dataMap = data.getValue() == null ? new HashMap<String, Object>() : (Map<String, Object>) data.getValue();
                // key is actibityID + percent threasholf
                if (dataMap.containsKey(humanTask.getId() + "_" + rule.percentThreashold)) {
                    collectAnalysisExecution.addExplanation(false, "Variable[" + variableName + "] contains key[" + humanTask.getId() + "_" + rule.percentThreashold + "] (already executed)");
                    return true;
                }
                collectAnalysisExecution.addExplanation(false, "Variable[" + variableName + "] does not contains key[" + humanTask.getId() + "_" + rule.percentThreashold + "] (to be executed)");
                return false;
            }
            // based on the delay ?
            else if (delaiRegistrationInMn != null && delaiRegistrationInMn > 0) {
                // event must apply between now and now -  delaiRegistrationInMn
                long currentTime = System.currentTimeMillis();
                // 
                //     TIME-TO-EXECUTE  <   now     < TIME-TO-EXECUTE + 12 mn
                // --> yes, execute it
                if (currentTime < timeToExecute || currentTime > timeToExecute + delaiRegistrationInMn) {
                    collectAnalysisExecution.addExplanation(false, "currentTime[" + currentTime + "] NOT in the period [" + delaiRegistrationInMn + "] timeToExecute is[" + timeToExecute + "] (not executed)");

                    return true;
                }

                collectAnalysisExecution.addExplanation(false, "currentTime[" + currentTime + "] In the period [" + delaiRegistrationInMn + "] timeToExecute is[" + timeToExecute + "] (to be executed)");
                return false;
            }

            ExceptionWithEvents ev = new ExceptionWithEvents();
            ev.listEvents.add(EVENT_NO_MECHANISM_REGISTER);
            throw ev;

        } catch (ClassCastException ec) {
            ExceptionWithEvents ev = new ExceptionWithEvents();
            ev.e = ec;
            ev.listEvents.add(new BEvent(EVENT_VARIABLE_REGISTRATON_CLASS_ERROR, "VariableName[" + variableName + "] Class[" + (data == null ? "null" : data.getValue().getClass().getName()) + "] processId[" + humanTask.getParentProcessInstanceId() + "]"));
            throw ev;
        } catch (DataNotFoundException ed) {
            ExceptionWithEvents ev = new ExceptionWithEvents();
            ev.e = ed;
            ev.listEvents.add(new BEvent(EVENT_VARIABLE_REGISTRATION_NOT_FOUND, "VariableName[" + variableName + "] processId[" + humanTask.getParentProcessInstanceId() + "]"));
            throw ev;
        }

    }

    /**
     * update the case to reference the rule was executed
     * 
     * @param humanTask
     * @param rule
     * @param input
     * @param processAPI
     */
    @SuppressWarnings("rawtypes")
    private void updateCase(HumanTaskInstance humanTask, Rule rule, MilkJobExecution input, ProcessAPI processAPI) throws ExceptionWithEvents {
        String variableName = input.getInputStringParameter(cstParamProcessVariableRegistration);
        DataInstance data = null;
        try {
            if (variableName != null && variableName.trim().length() > 0) {
                // based on a variable
                data = processAPI.getProcessDataInstance(variableName, humanTask.getParentProcessInstanceId());
                @SuppressWarnings("unchecked")
                Map<String, Object> dataMap = data.getValue() == null ? new HashMap<String, Object>() : (Map<String, Object>) data.getValue();
                // key is actibityID + percent threasholf
                Date date = new Date();
                dataMap.put(humanTask.getId() + "_" + rule.percentThreashold, "Execute at" + date.toString());
                processAPI.updateProcessDataInstance(variableName, humanTask.getParentProcessInstanceId(), (HashMap) dataMap);
            }
            // based on the delay registration ? Nothing to do.
        } catch (DataNotFoundException ed) {
            ExceptionWithEvents ev = new ExceptionWithEvents();
            ev.e = ed;
            ev.listEvents.add(new BEvent(EVENT_VARIABLE_REGISTRATION_NOT_FOUND, "VariableName[" + variableName + "] processId[" + humanTask.getParentProcessInstanceId() + "]"));
            throw ev;

        } catch (UpdateException eu) {
            ExceptionWithEvents ev = new ExceptionWithEvents();
            ev.e = eu;
            ev.listEvents.add(new BEvent(EVENT_VARIABLE_UPDATE_FAILED, "VariableName[" + variableName + "] processId[" + humanTask.getParentProcessInstanceId() + "] updateFail[" + eu.getMessage()));
            throw ev;

        }

    }

    /**
     * execute the rule
     * 
     * @param humanTask
     * @param rule
     * @param input
     * @param processAPI
     * @return
     */
    private List<BEvent> executeRule(HumanTaskInstance humanTask, Rule rule, MilkJobExecution input, ProcessAPI processAPI, IdentityAPI identityAPI) {
        List<BEvent> listEvents = new ArrayList<BEvent>();
        String userName = "";
        try {
            if (rule.getAction() == ACTION.EMAILUSER) {
                userName = rule.getActionParameters();
                User user = identityAPI.getUserByUserName(userName);
                ContactData contactData = identityAPI.getUserContactData(user.getId(), false);
                SendMail sendMail = new SendMail(input);
                String subject = resolvePlaceHolder(humanTask, user, rule, input, input.getInputStringParameter(cstParamEmailSubject));
                String content = resolvePlaceHolder(humanTask, user, rule, input, input.getInputStringParameter(cstParamEmailContent));

                String emailFrom = input.getInputStringParameter(cstParamEmailFrom);

                listEvents.addAll(sendMail.sendMail(contactData.getEmail(), emailFrom, subject, content));
            } else if (rule.getAction() == ACTION.EMAILCANDIDATES) {
                List<User> listUsers = processAPI.getPossibleUsersOfPendingHumanTask(humanTask.getId(), 0, 10000);
                for (User user : listUsers) {
                    ContactData contactData = identityAPI.getUserContactData(user.getId(), false);
                    SendMail sendMail = new SendMail(input);
                    String subject = resolvePlaceHolder(humanTask, user, rule, input, input.getInputStringParameter(cstParamEmailSubject));
                    String content = resolvePlaceHolder(humanTask, user, rule, input, input.getInputStringParameter(cstParamEmailContent));
                    listEvents.addAll(sendMail.sendMail(contactData.getEmail(), "bonitasoft@SLA.com", subject, content));

                }

            } else if (rule.getAction() == ACTION.ASSIGNUSER) {
                userName = rule.getActionParameters();
                User user = identityAPI.getUserByUserName(userName);
                processAPI.assignUserTask(humanTask.getId(), user.getId());
            }

            else if (rule.getAction() == ACTION.ASSIGNSUPERVISOR) {
                Integer level = TypesCast.getInteger(rule.getActionParameters(), 1);
                List<User> listUsers = processAPI.getPossibleUsersOfPendingHumanTask(humanTask.getId(), 0, 10000);
                User user = listUsers.size() > 0 ? listUsers.get(0) : null;
                if (user == null)
                    listEvents.add(new BEvent(EVENT_NO_USER, "Task [" + humanTask.getName() + "] Id[" + humanTask.getId() + "]"));
                else {
                    if (level > 10)
                        level = 10;

                    while (level > 0) {
                        level--;
                        long userManagerId = user.getManagerUserId();
                        if (userManagerId == -1)
                            break;
                        user = identityAPI.getUser(userManagerId);
                    }
                    processAPI.assignUserTask(humanTask.getId(), user.getId());
                }
            }

            else if (rule.getAction() == ACTION.EMAILACTOR) {
                String actorName = rule.getActionParameters();
                if (actorName.length() == 0) {
                    ActorInstance actor = processAPI.getActor(humanTask.getActorId());
                    actorName = actor.getName();
                }
                List<Long> listUsersId = processAPI.getUserIdsForActor(humanTask.getProcessDefinitionId(), actorName, 0, 10000);
                for (Long userId : listUsersId) {
                    User user = identityAPI.getUser(userId);
                    ContactData contactData = identityAPI.getUserContactData(userId, false);
                    SendMail sendMail = new SendMail(input);
                    String subject = resolvePlaceHolder(humanTask, user, rule, input, input.getInputStringParameter(cstParamEmailSubject));
                    String content = resolvePlaceHolder(humanTask, user, rule, input, input.getInputStringParameter(cstParamEmailContent));
                    listEvents.addAll(sendMail.sendMail(contactData.getEmail(), "bonitasoft@SLA.com", subject, content));
                }
            } else
                listEvents.add(new BEvent(EVENT_UNKNOW_ACTION, "Rule[" + rule.getUniqueId() + "] Action[" + rule.action + "]"));
        } catch (UpdateException e) {
            listEvents.add(new BEvent(EVENT_ASSIGN_TASK_FAILED, "Rule [" + rule.getUniqueId() + " task[" + humanTask.getId() + "] user[" + userName + "]"));
        } catch (UserNotFoundException e) {
            listEvents.add(new BEvent(EVENT_USER_NOT_FOUND, "Rule [" + rule.getUniqueId() + " task[" + humanTask.getId() + "] user[" + userName + "]"));

        } catch (ActorNotFoundException e) {
            listEvents.add(new BEvent(EVENT_ACTOR_NOT_FOUND, "Rule [" + rule.getUniqueId() + " task[" + humanTask.getId() + "]"));
        }
        return listEvents;
    }

    /**
     * resolve the place holder from a human task
     * 
     * @param humanTask
     * @param rule
     * @param text
     * @return
     */
    private String resolvePlaceHolder(HumanTaskInstance humanTask, User user, Rule rule, MilkJobExecution input, String text) {
        String emailBonitaServer = input.getInputStringParameter(cstParamEmailBonitaServer);
        Map<String, Object> placeHolder = PlaceHolder.getPlaceHolder(humanTask, emailBonitaServer);
        placeHolder.put("percentThreashold", String.valueOf(rule.percentThreashold));
        placeHolder.put("userFirstName", user.getFirstName());
        placeHolder.put("userLastName", user.getLastName());
        placeHolder.put("userTitle", user.getTitle());
        return PlaceHolder.replacePlaceHolder(placeHolder, text);

    }

}
