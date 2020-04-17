package org.bonitasoft.truckmilk.plugin;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.command.BonitaCommandDeployment;
import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.api.CommandAPI;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobExecution;

/* ******************************************************************************** */
/*                                                                                  */
/* Command */
/*                                                                                  */
/* Call a command */
/*                                                                                  */
/* ******************************************************************************** */

public class MilkCommand extends MilkPlugIn {

    private static PlugInParameter cstParamCommandName = PlugInParameter.createInstance("commandName", "Command name", TypeParameter.STRING, true, "Command name to call");
    private static PlugInParameter cstParamCommandParameters = PlugInParameter.createInstance("parameters", "Parameters", TypeParameter.JSON, true, "Parameters to send to the command");

    private SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

    public MilkCommand() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * plug in can check its environment, to detect if you missed something. An external component may
     * be required and are not installed.
     * 
     * @return a list of Events.
     */
    @Override
    public List<BEvent> checkPluginEnvironment(MilkJobExecution jobExecution) {
        return new ArrayList<>();
    }

    /**
     * check the environment of the job: is the command exist?
     */
    @Override
    public List<BEvent> checkJobEnvironment(MilkJobExecution jobExecution) {
        // is the command Exist ? 
        return new ArrayList<>();
    }

    /**
     * return the description of job
     */
    @Override
    public MilkPlugInDescription getDefinitionDescription() {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();

        plugInDescription.setName( "Command");
        plugInDescription.setLabel( "Command call");
        plugInDescription.setExplanation( "Call a Bonita command, with parameters");
        plugInDescription.setCategory(CATEGORY.OTHER);
        plugInDescription.addParameter(cstParamCommandName);
        plugInDescription.addParameter(cstParamCommandParameters);
        return plugInDescription;
    }

    /**
     * execution of the job. Just calculated the result according the parameters, and return it.
     */
    @Override
    public MilkJobOutput execute(MilkJobExecution jobExecution) {
        MilkJobOutput plugTourOutput = jobExecution.getMilkJobOutput();

        // if the date has to be added in the result ?
        String commandName = jobExecution.getInputStringParameter(cstParamCommandName);
        String commandParameters = jobExecution.getInputStringParameter(cstParamCommandParameters);

        // call the command

        plugTourOutput.executionStatus = ExecutionStatus.SUCCESS;
        if (jobExecution.pleaseStop())
            plugTourOutput.executionStatus = ExecutionStatus.SUCCESSPARTIAL;

        return plugTourOutput;
    }

    /**
     * call the command
     * 
     * @param commandName
     * @param commandParameters
     * @param tenantId
     * @param apiAccessor
     * @return
     */
    public Map<String, Object> callCommand(String commandName, HashMap<String, Serializable> commandParameters, long tenantId, APIAccessor apiAccessor) {
        CommandAPI commandAPI = apiAccessor.getCommandAPI();
        BonitaCommandDeployment bonitaCommand = BonitaCommandDeployment.getInstance(commandName);

        return bonitaCommand.callDirectCommand(commandParameters, tenantId, commandAPI);

    }
}
