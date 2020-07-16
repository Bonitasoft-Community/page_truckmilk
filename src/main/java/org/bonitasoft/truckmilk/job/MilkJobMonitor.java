package org.bonitasoft.truckmilk.job;

import java.util.Date;
import java.util.Set;

import org.bonitasoft.truckmilk.job.MilkJob.TrackExecution;
import org.bonitasoft.truckmilk.toolbox.TypesCast;

/* ******************************************************************************** */
/*                                                                                  */
/* Monitor externaly an job execution */
/*                                                                                  */
/*                                                                                  */
/* ******************************************************************************** */

public class MilkJobMonitor {
    private static String cstTruckMilkThreadName ="Bonita-Truckmilk-";

    MilkJob milkJob = null;
    public MilkJobMonitor( MilkJob milkJob) {
        this.milkJob = milkJob;
    }
    /**
     * 
     * @return
     */
    public String getThreadName() {
        return cstTruckMilkThreadName +milkJob.getIdJob();
    }
    /**
     * 
     * @return
     */
    public String getThreadDump() {
        String threadNameSearched = getThreadName();
        StringBuilder result = new StringBuilder();
        result.append("<table class=\"table table-striped table-hover table-condensed\">");
        result.append("<tr><td>Thread Name</td><td>"+threadNameSearched+"</td></tr>");
        TrackExecution trackExecution = milkJob.getTrackExecution();
        if (trackExecution!=null) {
            result.append("<tr><td>Information</td><td>"+trackExecution.avancementInformation+"</td></tr>");
            result.append("<tr><td>Started</td><td>"+ (trackExecution.startTime==0 ? "" : TypesCast.getHumanDate( new Date(trackExecution.startTime)))+"</td></tr>");
            result.append("<tr><td>% advancement</td><td >"+trackExecution.percent+" % </td></tr>");
            result.append("<tr><td>Estimation duration</td><td>"+ TypesCast.getHumanDuration(trackExecution.endTimeEstimatedInMs, false)+"</td></tr>");
            result.append("<tr><td>Hostname</td><td>"+ trackExecution.inExecutionHostName+"</td></tr>");
        }
        
        boolean foundThread=false;
        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        for (Thread th : threadSet) {
            // Bonita-Worker-1-10
            if (th.getName().equals(threadNameSearched)) {
                foundThread=true;
                result.append("<tr><td>Thread State</td><td>"+ th.getState().toString()+"</td></tr>");
                
                StackTraceElement[] listSt = th.getStackTrace();
                /* go from the lower to the Upper level */
                result.append("<tr><td colspan=\"2\">");
                result.append("<h3>Thread Dump</h3>");
                for (int i=0;i<listSt.length;i++ )
                {
                  StackTraceElement st = listSt[  i ];
                  result.append("at "+st.getClassName()+".<i>"+st.getMethodName()+"</i>: "+st.getLineNumber()+"<br>");                  
                }
                result.append("</td></tr>");
                return result.toString();
            }
        }
        if (!foundThread)
            result.append("<tr><td colspan=\"2\">Thread does not exist on this machine (Terminated, or on an another cluster's node)?)</td></tr>");
        result.append("</table>");
        return result.toString();
    }
}
