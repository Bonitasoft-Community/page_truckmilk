package org.bonitasoft.truckmilk.plugin;

import java.util.ArrayList;
import java.util.List;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.SendMailEnvironment;

public class MilkMail extends MilkPlugIn {

    public MilkMail() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * check the environment : for the milkEmailUsersTasks, we require to be able to send an email
     */
    public List<BEvent> checkPluginEnvironment(MilkJobExecution milkJobExecution) {
        return SendMailEnvironment.checkEnvironment(milkJobExecution, this);
    };

    /**
     * check the Job's environment
     */
    public List<BEvent> checkJobEnvironment(MilkJobExecution milkJobExecution) {
        return new ArrayList<>();

    };

    @Override
    public MilkJobOutput execute(MilkJobExecution input) {
        return input.getMilkJobOutput();
        
    }

    @Override
    public MilkPlugInDescription getDefinitionDescription(MilkJobContext milkJobContext) {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();
        plugInDescription.setName( "MonitorMail");
        plugInDescription.setLabel( "Monitor Email");
        plugInDescription.setExplanation( "Monitor a email adress. When a email arrive on the mail, if it match the properties, a new case is created or a new task is executed");
        plugInDescription.setCategory( CATEGORY.CASES );
        return plugInDescription;
    }
}
