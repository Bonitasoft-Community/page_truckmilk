package org.bonitasoft.truckmilk.engine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.ReplacementPlugIn;
import org.bonitasoft.truckmilk.job.MilkJobContext;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.plugin.cases.MilkCancelCases;
import org.bonitasoft.truckmilk.plugin.cases.MilkCleanArchivedDross;
import org.bonitasoft.truckmilk.plugin.cases.MilkDeleteCases;
import org.bonitasoft.truckmilk.plugin.cases.MilkMoveArchive;
import org.bonitasoft.truckmilk.plugin.cases.MilkPurgeArchive;
import org.bonitasoft.truckmilk.plugin.monitor.MilkDirectory;
import org.bonitasoft.truckmilk.plugin.monitor.MilkGrumman;
import org.bonitasoft.truckmilk.plugin.monitor.MilkLogObserver;
import org.bonitasoft.truckmilk.plugin.monitor.MilkRadarBonitaEngine;
import org.bonitasoft.truckmilk.plugin.other.MilkMeteor;
import org.bonitasoft.truckmilk.plugin.other.MilkPing;
import org.bonitasoft.truckmilk.plugin.processes.MilkDeleteProcesses;
import org.bonitasoft.truckmilk.plugin.processes.MilkSonar;
import org.bonitasoft.truckmilk.plugin.tasks.MilkDeleteDuplicateTasks;
import org.bonitasoft.truckmilk.plugin.tasks.MilkEmailUsersTasks;
import org.bonitasoft.truckmilk.plugin.tasks.MilkReplayFailedTask;
import org.bonitasoft.truckmilk.plugin.tasks.MilkRestartFlowNodes;
import org.bonitasoft.truckmilk.plugin.tasks.MilkSLA;
import org.bonitasoft.truckmilk.plugin.tasks.MilkUnassignTasks;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

/**
 * Contains all PlugIn (not the tourn only the plug in)
 * Nota: first version, list plug in is STATIC, so the object can be created from the Command or
 * from the Client. When the list will be dynamic,
 * this can be done only at the command level.
 */
public class MilkPlugInFactory {

    static MilkLog logger = MilkLog.getLogger(MilkPlugInFactory.class.getName());

    private static BEvent eventInternalError = new BEvent(MilkPlugInFactory.class.getName(), 1, Level.ERROR,
            "Internal error", "Internal error, check the log");
    private static BEvent eventNotInitialized = new BEvent(MilkPlugInFactory.class.getName(), 2, Level.ERROR,
            "Not initialized", "Factory is not initialized", "PlugIn does not work", "Call administrator");

    
    private MilkJobContext milkJobContext;
    private List<MilkPlugIn> listPlugIns = new ArrayList<>();

    private List<BEvent> listInitialiseEvent = new ArrayList<>();

    /**
     * get an factory object. If the factory initialisation failed, getInitialiseStatus return a
     * status
     * 
     * @param tenantId
     * @return
     */
    public static MilkPlugInFactory getInstance(MilkJobContext milkJobContext ) {
        MilkPlugInFactory milkPlugInFactory = new MilkPlugInFactory( milkJobContext);
        milkPlugInFactory.initialise( milkJobContext);

        return milkPlugInFactory;
    }

    private MilkPlugInFactory(MilkJobContext milkJobContext) {
        this.milkJobContext =  milkJobContext;
        listInitialiseEvent.add(eventNotInitialized);

    }

    public MilkJobContext getMilkJobContext() {
        return milkJobContext;
    }

    /**
     * this is the list of all embedded plug in (given with the truck milk)
     * 
     * @param tenantId
     * @return
     */
    private void collectListPlugIn(MilkJobContext milkJobContext) {
        listPlugIns = new ArrayList<>(); // clean it
        listPlugIns.add(new MilkDeleteCases());
        listPlugIns.add(new MilkDirectory());
        listPlugIns.add(new MilkEmailUsersTasks());
        // listPlugIns.add(new MilkMail());
        listPlugIns.add(new MilkPing());
        listPlugIns.add(new MilkPurgeArchive());
        listPlugIns.add(new MilkCleanArchivedDross() );
        listPlugIns.add(new MilkReplayFailedTask());
        listPlugIns.add(new MilkSLA());
        listPlugIns.add(new MilkUnassignTasks() );
        listPlugIns.add(new MilkMeteor() );
        listPlugIns.add(new MilkRestartFlowNodes() );
        listPlugIns.add(new MilkGrumman() );
        listPlugIns.add(new MilkCancelCases() );
        listPlugIns.add(new MilkRadarBonitaEngine() );
        listPlugIns.add(new MilkDeleteDuplicateTasks() );
        listPlugIns.add(new MilkLogObserver() );
        listPlugIns.add(new MilkDeleteProcesses() );
        listPlugIns.add(new MilkSonar() );
        listPlugIns.add(new MilkMoveArchive() );
    }

    /**
     * Get the plug in from the name
     * Some plug in are deprecated, and replaced by new one
     * @param name
     * @return
     */
    public MilkPlugIn getPluginFromName(String name) {
        for (MilkPlugIn plugIn : listPlugIns)
            if (plugIn.getName().equals(name))
                return plugIn;
       
        return null;
    }

   
    /**
     * get a replacement if exist, then update parameters
     * @param plugInName
     * @param parameterMap
     * @return
     */
    public ReplacementPlugIn getReplacement( String plugInName, Map<String,Object> parameterMap ) {
        for (MilkPlugIn plugIn : listPlugIns) {
            ReplacementPlugIn replacement = plugIn.getReplacement( plugInName, parameterMap);
            if (replacement != null)
                return replacement;
        }
        return null;
    }
    public List<MilkPlugIn> getListPlugIn() {
        return listPlugIns;
    }

    public List<BEvent> initialise(MilkJobContext milkJobContext) {
        listInitialiseEvent = new ArrayList<>();
        collectListPlugIn( milkJobContext );
        try {
            MilkJobExecution milkJobExecution = new MilkJobExecution(  milkJobContext);
            for (MilkPlugIn plugIn : listPlugIns) {
                // call the plug in, and init all now
                listInitialiseEvent.addAll(plugIn.initialize( milkJobExecution));
            }
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe("ERROR " + e + " at " + exceptionDetails);

            listInitialiseEvent.add(new BEvent(eventInternalError, e.getMessage()));

        } catch (Error er) {
            StringWriter sw = new StringWriter();
            er.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe("ERROR " + er + " at " + exceptionDetails);

            listInitialiseEvent.add(new BEvent(eventInternalError, er.getMessage()));
        }
        return listInitialiseEvent;
    }

    /**
     * return the initalize status
     * 
     * @return
     */
    public List<BEvent> getInitaliseStatus() {
        return listInitialiseEvent;
    }
}
