package org.bonitasoft.truckmilk.schedule;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.bpm.flownode.TimerType;
import org.bonitasoft.engine.bpm.process.ActivationState;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfo;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfoSearchDescriptor;
import org.bonitasoft.engine.bpm.process.impl.AutomaticTaskDefinitionBuilder;
import org.bonitasoft.engine.bpm.process.impl.ProcessDefinitionBuilder;
import org.bonitasoft.engine.bpm.process.impl.StartEventDefinitionBuilder;
import org.bonitasoft.engine.exception.DeletionException;
import org.bonitasoft.engine.expression.Expression;
import org.bonitasoft.engine.expression.ExpressionBuilder;
import org.bonitasoft.engine.expression.ExpressionType;
import org.bonitasoft.engine.operation.LeftOperand;
import org.bonitasoft.engine.operation.LeftOperandBuilder;
import org.bonitasoft.engine.operation.OperationBuilder;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.schedule.MilkSchedulerInt.TypeScheduler;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

import org.bonitasoft.engine.api.ProcessAPI;

public class MilkSchedulerProcessTimer extends MilkSchedulerInt {

    private static MilkLog logger = MilkLog.getLogger(MilkScheduleQuartz.class.getName());
    private static String logHeader = "MilkScheduleQuartz ~~ ";
    private APIAccessor apiAccessor;

    private static BEvent eventCantRemoveExistingProcess = new BEvent(MilkSchedulerProcessTimer.class.getName(), 1, Level.ERROR,
            "Can't remove existing process", "Scheduler is based on a process. The current process can't be removed.", "Scheduler is not reset", "Check the exeption. Remove the process by an administrator's action");

    private static BEvent eventErrorSearchingProcess = new BEvent(MilkSchedulerProcessTimer.class.getName(), 2, Level.ERROR,
            "Can't find the process", "Scheduler is based on a process. It should be deployed, but it's not possible to find it", "Scheduler is not reset", "Search the process");
    private static BEvent eventProcessIsNotDeployed = new BEvent(MilkSchedulerProcessTimer.class.getName(), 3, Level.APPLICATIONERROR,
            "Process is not deployed", "Scheduler is based on a process. It should be deployed", "Startup is not effective", "Do a reset");

    private static BEvent eventEnableFailed = new BEvent(MilkSchedulerProcessTimer.class.getName(), 4, Level.APPLICATIONERROR,
            "Process Can't be enabled", "Scheduler is based on a process. Enable this process failed", "Startup is not effective", "Check the process, try to enable by an administator action");
    private static BEvent eventDisableFailed = new BEvent(MilkSchedulerProcessTimer.class.getName(), 5, Level.APPLICATIONERROR,
            "Process Can't be disabled", "Scheduler is based on a process. Disable this process failed", "Shutdown is not effective", "Check the process, try to enable by an administator action");

    private static BEvent eventProcessIsNotEnabled = new BEvent(MilkSchedulerProcessTimer.class.getName(), 6, Level.INFO,
            "Process Is not enable", "Process is not enable, scheduler is stopped");

    private static BEvent eventSchedulerStarted = new BEvent(MilkSchedulerProcessTimer.class.getName(), 7, Level.SUCCESS,
            "Scheduler is started", "Scheduler is up and running");

    private static BEvent eventSchedulerStopped = new BEvent(MilkSchedulerProcessTimer.class.getName(), 8, Level.SUCCESS,
            "Scheduler is stopped", "Scheduler is stopped");

    public MilkSchedulerProcessTimer(MilkSchedulerFactory factory, APIAccessor apiAccessor) {
        super(factory);
        this.apiAccessor = apiAccessor;
    }

    @Override
    public boolean needRestartAtInitialization() {
        return false;
    }

    /**
     * 
     */
    @Override
    public List<BEvent> startup(long tenantId, boolean forceReset) {
        List<BEvent> listEvents = new ArrayList<>();

        ProcessDeploymentInfo processDeployment = getProcessTruckMilk();
        if (processDeployment == null) {
            listEvents.add(eventProcessIsNotDeployed);
            return listEvents;
        }
        if (processDeployment.getActivationState().equals(ActivationState.DISABLED))
            try {
                apiAccessor.getProcessAPI().enableProcess(processDeployment.getProcessId());
            } catch (Exception e) {
                listEvents.add(new BEvent(eventEnableFailed, "Process [" + CST_PROCESSNAME + "] PID[" + processDeployment.getProcessId() + "] enable failed " + e.getMessage()));
                return listEvents;
            }
        listEvents.add(eventSchedulerStarted);
        return listEvents;
    }

    /**
     * 
     */
    @Override
    public List<BEvent> shutdown(long tenantId) {
        List<BEvent> listEvents = new ArrayList<>();

        ProcessDeploymentInfo processDeployment = getProcessTruckMilk();
        if (processDeployment == null) {
            listEvents.add(eventProcessIsNotDeployed);
            return listEvents;
        }
        if (processDeployment.getActivationState().equals(ActivationState.ENABLED))
            try {
                apiAccessor.getProcessAPI().disableProcess(processDeployment.getProcessId());
            } catch (Exception e) {
                listEvents.add(new BEvent(eventDisableFailed, "Process [" + CST_PROCESSNAME + "] PID[" + processDeployment.getProcessId() + "] enable failed " + e.getMessage()));
                return listEvents;
            }
        listEvents.add(eventSchedulerStopped);
        return listEvents;
    }

    @Override
    public List<BEvent> check(long tenantId) {
        List<BEvent> listEvents = new ArrayList<>();

        ProcessDeploymentInfo processDeployment = getProcessTruckMilk();
        if (processDeployment == null) {
            listEvents.add(eventProcessIsNotDeployed);
            return listEvents;
        }
        if (processDeployment.getActivationState().equals(ActivationState.DISABLED)) {
            listEvents.add(eventProcessIsNotEnabled);
        }
        return listEvents;
    }

    @Override
    public StatusScheduler getStatus(long tenantId) {
        StatusScheduler statusScheduler = new StatusScheduler();

        statusScheduler.listEvents = check(tenantId);
        if (BEventFactory.isError(statusScheduler.listEvents)) {
            statusScheduler.status = TypeStatus.SHUTDOWN;
        } else
            statusScheduler.status = TypeStatus.STARTED;
        return statusScheduler;
    }

    @Override
    public List<BEvent> operation(Map<String, Serializable> parameters) {
        return null;
    }

    @Override
    public TypeScheduler getType() {
        return TypeScheduler.PROCESS;

    }

    @Override
    public Date getDateNextHeartBeat(long tenantId) {
        // with Process, no idea at this moment
        return null;
    }

    @Override
    public String getDescription() {
        return "A Technical process, TruckMilkHeartBeat, is created and deployed";
    }

    private final static String CST_PROCESSNAME = "TruckMilkHeartBeat";

    @Override
    public List<BEvent> checkAndDeploy(boolean forceDeploy, File pageDirectory, long tenantId) {
        List<BEvent> listEvents = new ArrayList<>();

        // Create the process
        ProcessDeploymentInfo processDeployment = getProcessTruckMilk();
        if (processDeployment == null) {
            listEvents.addAll(generateTheProcess());
            if (BEventFactory.isError(listEvents))
                return listEvents;
            processDeployment = getProcessTruckMilk();
        }
        // enable it
        if (processDeployment == null) {
            listEvents.add(new BEvent(eventErrorSearchingProcess, "Process [" + CST_PROCESSNAME + "]"));
            return listEvents;

        }
        if (processDeployment.getActivationState().equals(ActivationState.DISABLED))
            listEvents.addAll(startup(tenantId, false));
        return null;
    }

    /**
     * 
     */
    @Override
    public List<BEvent> reset(long tenantId) {
        List<BEvent> listEvents = new ArrayList<>();

        ProcessDeploymentInfo processDeployment = getProcessTruckMilk();
        ProcessAPI processAPI = apiAccessor.getProcessAPI();
        if (processDeployment != null) {
            try {
                processAPI.deleteProcessDefinition(processDeployment.getProcessId());
            } catch (DeletionException e) {
                listEvents.add(new BEvent(eventCantRemoveExistingProcess, "Process [" + CST_PROCESSNAME + "] PID[" + processDeployment.getProcessId() + "] Exception: " + e.getMessage()));
                return listEvents;
            }
        }
        listEvents.addAll(generateTheProcess());
        return listEvents;
    }

    private final static String CST_CALLCOMMAND = "import java.util.logging.Logger;" +
            "import org.bonitasoft.engine.api.CommandAPI;\n" +
            "import org.bonitasoft.engine.command.CommandCriterion;\n" +
            "import org.bonitasoft.engine.command.CommandDescriptor;\n" +

            "Logger logger = Logger.getLogger(\"org.bonitasoft.truckmilk.processCallHeartBeat\");\n" +
            "logger.info(\"Call Truckmilk command\");\n" +
            "CommandAPI commandAPI = apiAccessor.getCommandAPI();\n" +
            "CommandDescriptor commandTruckMilk = getCommandByName(\"truckmilk\", commandAPI);\n" +

            "Map<String, Serializable> parameters = new HashMap<String, Serializable>();\n" +
            "try {\n" +
            "    parameters.put(\"tenantId\", 1L);\n" +
            "    parameters.put(\"verb\", \"HEARTBEAT\");\n" +

            "    final Serializable resultCommand = commandAPI.execute(commandTruckMilk.getId(), parameters);\n" +
            "    logger.info(\"End Call Truckmilk command with success\");\n" +
            "    return \"done\";\n" +

            "} catch (final Exception e) {\n" +
            "    logger.severe(\"~~~~~~~~~~  : ERROR When call command[truckmilk] \" + e.getMessage() );\n" +
            "    return \"fail \"+e.getMessage();\n" +
            "}\n" +
            "private  CommandDescriptor getCommandByName(String commandName, CommandAPI commandAPI) {\n" +
            "    List<CommandDescriptor> listCommands = commandAPI.getAllCommands(0, 1000, CommandCriterion.NAME_ASC);\n" +
            "    for (CommandDescriptor command : listCommands) {\n" +
            "        if (commandName.equals(command.getName()))\n" +
            "            return command;\n" +
            "    }\n" +
            "    return null;\n" +

            "}";
    private final static String CST_PURGECASES = "";

    private ProcessDeploymentInfo getProcessTruckMilk() {
        try {
            SearchOptionsBuilder sob = new SearchOptionsBuilder(0, 100);
            sob.filter(ProcessDeploymentInfoSearchDescriptor.NAME, CST_PROCESSNAME);

            SearchResult<ProcessDeploymentInfo> searchProcess = apiAccessor.getProcessAPI().searchProcessDeploymentInfos(sob.done());
            if (searchProcess.getCount() > 0)
                return searchProcess.getResult().get(0);
        } catch (Exception e) {
            logger.severe("Can't search process [" + CST_PROCESSNAME + "] : " + e.getMessage());
        }
        return null;

    }

    private List<BEvent> generateTheProcess() {
        List<BEvent> listEvents = new ArrayList<>();
        try {
            final ProcessDefinitionBuilder design = new ProcessDefinitionBuilder().createNewInstance(CST_PROCESSNAME, "1.0");

            design.addShortTextData("SaveCommandExecution", null);

            StartEventDefinitionBuilder startEvent = design.addStartEvent("Timer");
            // expression is 00:01:00
            Expression timerValue = new ExpressionBuilder().createConstantStringExpression("00:01:00");
            startEvent.addTimerEventTriggerDefinition(TimerType.DURATION, timerValue);

            AutomaticTaskDefinitionBuilder callCommand = startEvent.addAutomaticTask("CallCommand");

            Expression executeCommandAPI = new ExpressionBuilder().createConstantStringExpression(CST_CALLCOMMAND);
            OperationBuilder operationCallCommand = new OperationBuilder();
            operationCallCommand.setLeftOperand((new LeftOperandBuilder()).createDataLeftOperand("SaveCommandExecution"));
            operationCallCommand.setRightOperand(executeCommandAPI);
            callCommand.addOperation(operationCallCommand.done());

            callCommand.addEndEvent("The End");

            apiAccessor.getProcessAPI().deploy(design.done());
        } catch (Exception e) {

        }
        return listEvents;
    }
}
