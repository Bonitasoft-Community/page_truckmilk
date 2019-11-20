package org.bonitasoft.truckmilk.plugin;

import java.util.ArrayList;
import java.util.List;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.SendMailEnvironment;

public class MilkMail extends MilkPlugIn {

    public MilkMail() {
        super(TYPE_PLUGIN.EMBEDED);
    }

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

    @Override
    public PlugTourOutput execute(MilkJobExecution input, APIAccessor apiAccessor) {
        PlugTourOutput plugTourOutput = input.getPlugTourOutput();
        return plugTourOutput;
    }

    @Override
    public PlugInDescription getDefinitionDescription() {
        PlugInDescription plugInDescription = new PlugInDescription();
        plugInDescription.name = "MonitorMail";
        plugInDescription.label = "Monitor Email";
        plugInDescription.description = "Monitor a email adress. When a email arrive on the mail, if it match the properties, a new case is created or a new task is executed";

        return plugInDescription;
    }
}
