package org.bonitasoft.truckmilk.engine;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.process.ActivationState;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfo;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfoSearchDescriptor;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.DELAYSCOPE;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter.FilterProcess;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.TypesCast;



public class MilkPlugInToolbox {
    


    /**
     * when the parameter is a TypeParameter.PROCESSNAME
     * @param processNameVersion
     * @param processAPI
     * @return
     * @throws SearchException
     */
    public static SearchResult<ProcessDeploymentInfo> getListProcessDefinitionId(String processNameVersion, FilterProcess filterProcess, ProcessAPI processAPI) throws SearchException {
        if (processNameVersion==null)
            processNameVersion="";
        processNameVersion = processNameVersion.trim();
        String processNameOnly = processNameVersion;
        String processVersionOnly = null;
        if (processNameVersion.endsWith(")")) {
            int firstParenthesis = processNameVersion.lastIndexOf('(');
            if (firstParenthesis != -1) {
                processNameOnly = processNameVersion.substring(0, firstParenthesis);
                processVersionOnly = processNameVersion.substring(firstParenthesis + 1, processNameVersion.length() - 1);
            }
        }
        SearchOptionsBuilder searchOption = new SearchOptionsBuilder(0, 1000);
        searchOption.filter(ProcessDeploymentInfoSearchDescriptor.NAME, processNameOnly.trim());
        if (processVersionOnly != null) {
            searchOption.filter(ProcessDeploymentInfoSearchDescriptor.VERSION, processVersionOnly.trim());
        }
        
        if (filterProcess == null || filterProcess.equals(FilterProcess.ALL)) 
        {} // no filter
        else if (filterProcess.equals(FilterProcess.ONLYENABLED))
                searchOption.filter(ProcessDeploymentInfoSearchDescriptor.ACTIVATION_STATE, ActivationState.ENABLED.toString());
        else if (filterProcess.equals(FilterProcess.ONLYDISABLED))
                searchOption.filter(ProcessDeploymentInfoSearchDescriptor.ACTIVATION_STATE, ActivationState.DISABLED.toString());
        
        return processAPI.searchProcessDeploymentInfos(searchOption.done());

    }
    
    
   
    
    /* ******************************************************************************** */
    /*                                                                                  */
    /* Create Case, Execute Tasks */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    public List<BEvent> createCase() {
        return new ArrayList<>();
    }

    public List<BEvent> executeTasks() {
        return new ArrayList<>();
    }

}
