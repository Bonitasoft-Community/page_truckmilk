package org.bonitasoft.truckmilk.job;

import java.util.Set;

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