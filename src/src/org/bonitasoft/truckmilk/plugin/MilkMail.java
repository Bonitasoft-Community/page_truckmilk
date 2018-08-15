package org.bonitasoft.truckmilk.plugin;

import java.util.Map;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.service.ServiceAccessor;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugInDescription;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugTourInput;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugTourOutput;

public class MilkMail extends MilkPlugIn {

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
        plugInDescription.name="MonitorMail";
        plugInDescription.displayName="Monitor Email";
        plugInDescription.description="Monitor a email adress. When a email arrive on the mail, if it match the properties, a new case is created or a new task is executed";
        
        return plugInDescription;
    }
}
