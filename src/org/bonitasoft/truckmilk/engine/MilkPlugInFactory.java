package org.bonitasoft.truckmilk.engine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.plugin.MilkDeleteCases;
import org.bonitasoft.truckmilk.plugin.MilkEmailUsersTasks;
import org.bonitasoft.truckmilk.plugin.MilkMeteor;
import org.bonitasoft.truckmilk.plugin.MilkPing;
import org.bonitasoft.truckmilk.plugin.MilkPurgeArchive;
import org.bonitasoft.truckmilk.plugin.MilkPurgeArchivedListGetList;
import org.bonitasoft.truckmilk.plugin.MilkPurgeArchivedListPurge;
import org.bonitasoft.truckmilk.plugin.MilkReplayFailedTask;
import org.bonitasoft.truckmilk.plugin.MilkRestartFlowNodes;
import org.bonitasoft.truckmilk.plugin.MilkSLA;
import org.bonitasoft.truckmilk.plugin.MilkUnassignTasks;
import org.bonitasoft.truckmilk.toolbox.MilkLog;

/**
 * Contains all PlugIn (not the tourn only the plug in)
 * Nota: first version, list plug in is STATIC, so the object can be created from the Command or
 * from the Client. When the list will be dynamic,
 * this can be done only at the command level.
 */
public class MilkPlugInFactory {

    static MilkLog logger = MilkLog.getLogger(MilkPlugInFactory.class.getName());

    private static BEvent EVENT_INTERNAL_ERROR = new BEvent(MilkPlugInFactory.class.getName(), 1, Level.ERROR,
            "Internal error", "Internal error, check the log");
    private static BEvent EVENT_NOT_INITIALIZED = new BEvent(MilkPlugInFactory.class.getName(), 2, Level.ERROR,
            "Not initialized", "Factory is not initialized", "PlugIn does not work", "Call administrator");

    private long tenantId;
    private List<MilkPlugIn> listPlugIns = new ArrayList<MilkPlugIn>();

    private List<BEvent> listInitialiseEvent = new ArrayList<BEvent>();

    /**
     * get an factory object. If the factory initialisation failed, getInitialiseStatus return a
     * status
     * 
     * @param tenantId
     * @return
     */
    public static MilkPlugInFactory getInstance(long tenantId) {
        MilkPlugInFactory milkPlugInFactory = new MilkPlugInFactory(tenantId);
        milkPlugInFactory.initialise();

        return milkPlugInFactory;
    }

    private MilkPlugInFactory(long tenantId) {
        this.tenantId = tenantId;
        listInitialiseEvent.add(EVENT_NOT_INITIALIZED);

    }

    public long getTenantId() {
        return tenantId;
    };

    /**
     * this is the list of all embedded plug in (given with the truck milk)
     * 
     * @param tenantId
     * @return
     */
    private void collectListPlugIn(long tenantId) {
        listPlugIns = new ArrayList<MilkPlugIn>(); // clean it
        listPlugIns.add(new MilkDeleteCases());
        // listPlugIns.add(new MilkDirectory());
        listPlugIns.add(new MilkEmailUsersTasks());
        // listPlugIns.add(new MilkMail());
        listPlugIns.add(new MilkPing());
        listPlugIns.add(new MilkPurgeArchive());
        listPlugIns.add(new MilkPurgeArchivedListPurge());
        listPlugIns.add(new MilkPurgeArchivedListGetList());
        listPlugIns.add(new MilkReplayFailedTask());
        listPlugIns.add(new MilkSLA());
        listPlugIns.add(new MilkUnassignTasks() );
        listPlugIns.add(new MilkMeteor() );
        listPlugIns.add(new MilkRestartFlowNodes() );
    }

    public MilkPlugIn getPluginFromName(String name) {
        for (MilkPlugIn plugIn : listPlugIns)
            if (plugIn.getName().equals(name))
                return plugIn;
        return null;
    }

    public List<MilkPlugIn> getListPlugIn() {
        return listPlugIns;
    }

    public List<BEvent> initialise() {
        listInitialiseEvent = new ArrayList<BEvent>();
        collectListPlugIn(tenantId);
        try {
            for (MilkPlugIn plugIn : listPlugIns) {
                // call the plug in, and init all now
                listInitialiseEvent.addAll(plugIn.initialize(tenantId));
            }
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe("ERROR " + e + " at " + exceptionDetails);

            listInitialiseEvent.add(new BEvent(EVENT_INTERNAL_ERROR, e.getMessage()));

        } catch (Error er) {
            StringWriter sw = new StringWriter();
            er.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe("ERROR " + er + " at " + exceptionDetails);

            listInitialiseEvent.add(new BEvent(EVENT_INTERNAL_ERROR, er.getMessage()));
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
