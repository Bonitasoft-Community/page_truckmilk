package org.bonitasoft.truckmilk.plugin;

import java.util.ArrayList;
import java.util.List;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.job.MilkJobExecution;

public class MilkDirectory extends MilkPlugIn {

    /**
     * 
     */
    public MilkDirectory() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * check the environment : for the milkDirectory, nothing is required
     * The path to check depends on the tour, so the only environment needed is to access a disk ?
     */
    public List<BEvent> checkEnvironment(long tenantId, APIAccessor apiAccessor) {
        return new ArrayList<BEvent>();
    };

    @Override
    public PlugTourOutput execute(MilkJobExecution input, APIAccessor apiAccessor) {
        PlugTourOutput plugTourOutput = input.getPlugTourOutput();

        return plugTourOutput;
    }

    @Override
    public PlugInDescription getDefinitionDescription() {
        PlugInDescription plugInDescription = new PlugInDescription();
        plugInDescription.name = "MonitorDirectory";
        plugInDescription.label = "Monitor Directory";
        plugInDescription.description = "Monitor a directory. When a file arrive in this directory, a new case is created";

        return plugInDescription;
    }
}
