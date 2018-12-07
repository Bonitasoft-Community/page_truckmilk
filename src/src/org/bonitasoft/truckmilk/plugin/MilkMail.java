package org.bonitasoft.truckmilk.plugin;

import java.util.List;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.truckmilk.toolbox.SendMail;
import org.bonitasoft.truckmilk.toolbox.SendMailEnvironment;

public class MilkMail extends MilkPlugIn {

  /**
   * they are embeded
   */
  public boolean isEmbeded() {
    return true;
  };

  /**
   * check the environment : for the milkEmailUsersTasks, we require to be able to send an email
   */
  public List<BEvent> checkEnvironment(long tenantId) {
    return SendMailEnvironment.checkEnvironment(tenantId);
  };

  @Override
  public PlugTourOutput execute(PlugTourInput input, APIAccessor apiAccessor) {
    PlugTourOutput plugTourOutput = input.getPlugTourOutput();
    return plugTourOutput;
  }

  @Override
  public PlugInDescription getDescription() {
    PlugInDescription plugInDescription = new PlugInDescription();
    plugInDescription.name = "MonitorMail";
    plugInDescription.displayName = "Monitor Email";
    plugInDescription.description = "Monitor a email adress. When a email arrive on the mail, if it match the properties, a new case is created or a new task is executed";

    return plugInDescription;
  }
}
