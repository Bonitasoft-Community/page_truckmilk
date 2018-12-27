package org.bonitasoft.truckmilk.plugin;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugTourOutput;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.TypeParameter;

/* ******************************************************************************** */
/*                                                                                  */
/* Ping                                                                             */
/*                                                                                  */
/* this class may be use as a skeleton for a new plug in                            */
/*                                                                                  */
/* ******************************************************************************** */

public class MilkPing extends MilkPlugIn {

  private static PlugInParameter cstParamAddDate = PlugInParameter.createInstance("addDate", TypeParameter.BOOLEAN, true, "If set, the date of execution is added in the status of execution");

  private static BEvent eventPing = new BEvent(MilkPing.class.getName(), 1, Level.INFO,
      "Ping !", "The ping job is executed correctly" );

  
  SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
  /**
   * plug in can check its environment, to detect if you missed something. An external component may be required and are not installed.
   * @return a list of Events.
   */
  @Override
  public List<BEvent> checkEnvironment(long tenantId) {
    return new ArrayList<BEvent>();
  }

  /**
   * return the description of ping job
   */
  @Override
  public PlugInDescription getDescription() {
    PlugInDescription plugInDescription = new PlugInDescription();
    
    plugInDescription.name = "Ping";
    plugInDescription.description="Just do a ping";
    plugInDescription.displayName = "Ping job";
    plugInDescription.addParameter(cstParamAddDate);
    return plugInDescription;
  }

  /**
   * execution of the job. Just calculed the result according the parameters, and return it.
   */
  @Override
  public PlugTourOutput execute(PlugTourInput input, APIAccessor apiAccessor) {
    PlugTourOutput plugTourOutput = input.getPlugTourOutput();

    // if the date has to be added in the result ?
    Boolean addDate = input.getInputBooleanParameter(cstParamAddDate.name, Boolean.TRUE);
    String parameters= addDate ? "Date: "+sdf.format( new Date()):"";
    plugTourOutput.addEvent( new BEvent( eventPing, parameters));
    plugTourOutput.executionStatus = ExecutionStatus.SUCCESS;
    return plugTourOutput;
  }

}
