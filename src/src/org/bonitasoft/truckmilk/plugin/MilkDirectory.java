package org.bonitasoft.truckmilk.plugin;

import java.util.Map;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.service.ServiceAccessor;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugInDescription;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugTourInput;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugTourOutput;

public class MilkDirectory extends MilkPlugIn {

    /**
     * 
     */
    public MilkDirectory() {
        super();
    }

    /**
     * they are embeded
     */
    public boolean isEmbeded() {
        return true;
    };
    @Override
    public PlugTourOutput execute(PlugTourInput input, APIAccessor apiAccessor) {
        PlugTourOutput plugTourOutput= input.getPlugTourOutput();

        return plugTourOutput;
    }

    @Override
    public PlugInDescription getDescription() {
        PlugInDescription plugInDescription = new PlugInDescription();
        plugInDescription.name="MonitorDirectory";
        plugInDescription.displayName="Monitor Directory";
        plugInDescription.description="Monitor a directory. When a file arrive in this directory, a new case is created";
        
        return plugInDescription;
    }
}
