package org.bonitasoft.truckmilk.plugin;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstance;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstancesSearchDescriptor;
import org.bonitasoft.engine.exception.DeletionException;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.TypesCast;

public class MilkPurgeArchivedListPurge extends MilkPlugIn {

    static Logger logger = Logger.getLogger(MilkPurgeArchivedListPurge.class.getName());

    private static BEvent eventDeletionSuccess = new BEvent(MilkPurgeArchivedListPurge.class.getName(), 1, Level.SUCCESS,
            "Deletion done with success", "Archived Cases are deleted with success");

    private static BEvent eventDeletionFailed = new BEvent(MilkPurgeArchivedListPurge.class.getName(), 2, Level.ERROR,
            "Error during deletion", "An error arrived during the deletion of archived cases", "Cases are not deleted", "Check the exception");
    
    private static BEvent EVENT_REPORT_ERROR = new BEvent(MilkPurgeArchivedListPurge.class.getName(), 3, Level.APPLICATIONERROR,
            "Error in source file", "The source file is not correct", "Check the source file, caseid is expected inside", "Check the error");

    private static PlugInParameter cstParamMaximumDeletionInCase = PlugInParameter.createInstance("maximumdeletionincase", "Maximum deletion in case", TypeParameter.LONG, 1000L, "Maximum case deleted in one execution, to not overload the engine. Maximum of 5000 is hardcoded");
    private static PlugInParameter cstParamMaximumDeletionInMinutes = PlugInParameter.createInstance("maximumdeletioninminutes", "Maximum time in Mn", TypeParameter.LONG, 3L, "Maximum time in minutes for the job. After this time, it will stop.");
    private static PlugInParameter cstParamSeparatorCSV = PlugInParameter.createInstance("separatorCSV", "Separator CSV", TypeParameter.STRING, ",", "CSV use a separator. May be ; or ,");
    private static PlugInParameter cstParamInputDocument = PlugInParameter.createInstanceFile("inputdocument", "Input List (CSV)", TypeParameter.FILEREAD, null, "List is a CSV containing caseid column and status column. When the status is 'DELETE', then the case is deleted\nExample: caseId;status\n342;DELETE\n345;DELETE", "ListToPurge.csv", "text/csv");

    public MilkPurgeArchivedListPurge() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * check the environment : for the milkEmailUsersTasks, we require to be able to send an email
     */
    public List<BEvent> checkEnvironment(long tenantId, APIAccessor apiAccessor) {
        return new ArrayList<BEvent>();
    };

    
    private class Statistic {
        long pleaseStopAfterManagedItems=0;
        long countIgnored=0;
        long countAlreadyDone=0;        
        long countBadDefinition=0;
        long countStillToAnalyse=0;
        long countNbItems=0;
        long totalLineCsv = 0;
    }
    @Override
    public PlugTourOutput execute(MilkJobExecution jobExecution, APIAccessor apiAccessor) {

        PlugTourOutput plugTourOutput = jobExecution.getPlugTourOutput();

        ProcessAPI processAPI = apiAccessor.getProcessAPI();
        // get Input 
        ByteArrayOutputStream outputByte = new ByteArrayOutputStream();
        jobExecution.getStreamParameter(cstParamInputDocument, outputByte);
        if (outputByte.size() == 0) {
            // no document uploaded
            plugTourOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
            return plugTourOutput;
        }
        Statistic statistic = new Statistic();

        jobExecution.setPleaseStopAfterTime(jobExecution.getInputLongParameter(cstParamMaximumDeletionInMinutes), 24 * 60L);
        jobExecution.setPleaseStopAfterManagedItems( jobExecution.getInputLongParameter(cstParamMaximumDeletionInCase), 5000L);
        statistic.pleaseStopAfterManagedItems = jobExecution.getPleaseStopAfterManagerItems();
        
        String separatorCSV = jobExecution.getInputStringParameter(cstParamSeparatorCSV);
        
        statistic.totalLineCsv = nbLinesInCsv(outputByte);
        List<Long> sourceProcessInstanceIds = new ArrayList<Long>();
        try {
            jobExecution.setAvancementTotalStep(statistic.totalLineCsv);
            
            BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(outputByte.toByteArray())));
            String line = reader.readLine();
            // read the header
            String[] header = line == null ? new String[0] : line.split(";");
            
            long lineNumber = 1;
            StringBuffer analysis = new StringBuffer();
            while ((line = reader.readLine()) != null) {

                if (jobExecution.pleaseStop()) {
                    analysis.append("Stop asked;");
                    break;
                }
             
                jobExecution.setAvancementStep(lineNumber);
                lineNumber++;

                Map<String, String> record = getMap(header, line, separatorCSV);
                if (record == null)
                    continue;
                Long caseId = TypesCast.getLong(record.get(MilkPurgeArchivedListGetList.cstColCaseId), null);
                String status = TypesCast.getString(record.get(MilkPurgeArchivedListGetList.cstColStatus), null);

                if (caseId == null) {
                    analysis.append("Line[" + lineNumber + "] " + MilkPurgeArchivedListGetList.cstColCaseId + " undefined;");
                    statistic.countBadDefinition++;
                }
                else if (status == null) {
                    statistic.countIgnored++;
                    // analysis.append("Line[" + lineNumber + "] " + MilkPurgeArchivedListGetList.cstColStatus + " undefined;");
                }
                else if ("DELETE".equalsIgnoreCase(status)) {     
                    
                    // delete it
                    sourceProcessInstanceIds.add(caseId);
                    if (sourceProcessInstanceIds.size() > 50) {
                        // purge now
                        long nbCaseDeleted = purgeList( sourceProcessInstanceIds, statistic, processAPI);    
                        plugTourOutput.nbItemsProcessed = statistic.countNbItems;
                        jobExecution.addManagedItem( nbCaseDeleted );
                        sourceProcessInstanceIds.clear();
                    }
                }
                else
                    statistic.countIgnored++;
            }
            // the end, purge a last time 
            if (sourceProcessInstanceIds.size() > 0) {
                long nbCaseDeleted = purgeList( sourceProcessInstanceIds, statistic, processAPI);    
                jobExecution.addManagedItem( nbCaseDeleted );
                plugTourOutput.nbItemsProcessed = statistic.countNbItems;
                sourceProcessInstanceIds.clear();
            }
            // calculated the last ignore  
            statistic.countStillToAnalyse += statistic.totalLineCsv - lineNumber;
            
            plugTourOutput.addEvent(new BEvent(eventDeletionSuccess, "AlreadyDone: "+statistic.countAlreadyDone+"; Purged:" + plugTourOutput.nbItemsProcessed+" Ignored(no status DELETED):"+statistic.countIgnored+" StillToAnalyse:"+statistic.countStillToAnalyse+" Bad Definition(noCaseid):"+statistic.countBadDefinition));
            plugTourOutput.executionStatus = (jobExecution.pleaseStop() || statistic.countStillToAnalyse>0) ? ExecutionStatus.SUCCESSPARTIAL : ExecutionStatus.SUCCESS;
            if (statistic.countBadDefinition>0)
            {
                plugTourOutput.addEvent(new BEvent(EVENT_REPORT_ERROR, " Bad Definition(noCaseid):"+statistic.countBadDefinition+" : "+analysis.toString()));
                plugTourOutput.executionStatus= ExecutionStatus.ERROR;    
            }
            
        } catch (IOException e) {
            logger.severe("Error Delete Archived ProcessInstance=[" + sourceProcessInstanceIds + "] Error[" + e.getMessage() + "]");
            plugTourOutput.executionStatus = ExecutionStatus.ERROR;
            plugTourOutput.addEvent(new BEvent(eventDeletionFailed, e, "Purge:" + sourceProcessInstanceIds));

        } catch (Exception e) {
            logger.severe("Error Delete Archived ProcessInstance=[" + sourceProcessInstanceIds + "] Error[" + e.getMessage() + "]");
            plugTourOutput.executionStatus = ExecutionStatus.ERROR;
            plugTourOutput.addEvent(new BEvent(eventDeletionFailed, e, "Purge:" + sourceProcessInstanceIds));
        }

        return plugTourOutput;
    }

    /**
     * 
     */
    @Override
    public PlugInDescription getDefinitionDescription() {
        PlugInDescription plugInDescription = new PlugInDescription();
        plugInDescription.name = "PurgeCaseFromList";
        plugInDescription.label = "Purge Archived Cases: Purge from list";
        plugInDescription.description = "Get a CSV File as input. Then, delete all case in the list where the column status ewuals DELETED";
        plugInDescription.addParameter(cstParamMaximumDeletionInCase);
        plugInDescription.addParameter(cstParamMaximumDeletionInMinutes);
        plugInDescription.addParameter(cstParamSeparatorCSV);
        plugInDescription.addParameter(cstParamInputDocument);
        /*
         * plugInDescription.addParameterFromMapJson(
         * "{\"delayinmn\":10,\"maxtentative\":12,\"processfilter\":[]}");
         */
        return plugInDescription;
    }

    /**
     * 
     * @param header
     * @param line
     * @return
     */
    private Map<String, String> getMap(String[] header, String line, String separatorCSV) {
        Map<String, String> record = new HashMap<String, String>();
        // don't use a StringTokenizer : if the line contains ;; for an empty information, StringTokenizer will merge the two separator

        List<String> listString = getStringTokenizerPlus(line, separatorCSV);
        for (int i = 0; i < header.length; i++) {
            record.put(header[i], i < listString.size() ? listString.get(i) : null);
        }
        return record;
    }

    /**
     * 
     * @param line
     * @param charSeparator
     * @return
     */
    private List<String> getStringTokenizerPlus(String line, final String charSeparator) {
        final List<String> list = new ArrayList<String>();
        int index = 0;
        if (line == null || line.length() == 0) {
            return list;
        }
        // now remove all empty string at the end of the list (keep the minimal)
        // then if string is "hello;this;;is;the;word;;;;"
        // line is reduce to "hello;this;;is;the;word"
        // nota : if the line is
        // then if string is "hello;this;;is;the;word;; ;;"
        // then "hello;this;;is;the;word;; "
        while (line.endsWith(";"))
            line = line.substring(0, line.length() - 1);
        while (index != -1) {
            final int nextPost = line.indexOf(charSeparator, index);
            if (nextPost == -1) {
                list.add(line.substring(index));
                break;
            } else {
                list.add(line.substring(index, nextPost));
            }
            index = nextPost + 1;
        }

        return list;
    }

    /** methid  processAPI.deleteArchivedProcessInstancesInAllStates(sourceProcessInstanceIds) is very long, even if there are nothing to purge
     * so, let's first search the real number to purge, and do the purge only on real case.
     * @return
     */

    public int purgeList(  List<Long> sourceProcessInstanceIds, Statistic statistic, ProcessAPI processAPI) throws DeletionException, SearchException    
    {
        long startTimeSearch = System.currentTimeMillis();
        SearchOptionsBuilder searchActBuilder = new SearchOptionsBuilder(0, sourceProcessInstanceIds.size());
        for (int i=0;i<sourceProcessInstanceIds.size();i++)
        {
            if (i>0)
                searchActBuilder.or();
            searchActBuilder.filter( ArchivedProcessInstancesSearchDescriptor.SOURCE_OBJECT_ID, sourceProcessInstanceIds.get( i ));
        }
        SearchResult<ArchivedProcessInstance> searchArchivedProcessInstance = processAPI.searchArchivedProcessInstances(searchActBuilder.done());
        long endTimeSearch = System.currentTimeMillis();
        List<Long> realId = new ArrayList<Long>();
        for (ArchivedProcessInstance archived : searchArchivedProcessInstance.getResult())
        {
            realId.add( archived.getSourceObjectId());
        }
        
        // we know how many item we don't need to process in this batch
        // BATCH TO STUDY                   : sourceProcessInstanceIds
        // Case To delete in this batch     : realId.size()
        // Already Managed in this job      : statistic.countNbItems
        // Already Done in a previous job   : sourceProcessInstanceIds.size() - realId.size() to add to countAlreadyDone
        // if (Already Managed in this job) + (Case To Delete in the bath) > pleaseStopAfterManagedItem then we have to limit our number of deletion 
        
        statistic.countAlreadyDone += sourceProcessInstanceIds.size() - realId.size();
        // ok, the point is now maybe we don't want to process ALL this process to delete, due to the limitation
        if (statistic.countNbItems + realId.size() > statistic.pleaseStopAfterManagedItems )
        {
            // too much, we need to reduce the number
            long maximumToManage = statistic.pleaseStopAfterManagedItems - statistic.countNbItems;
            // to be study after is then the realId.size - maximumToManage
            statistic.countStillToAnalyse += realId.size() - maximumToManage;
            realId = realId.subList(0, (int) maximumToManage);
        }
        
        long startTimeDelete = System.currentTimeMillis();
        long nbCaseDeleted=0;
        if (realId.size()>0)
        {
            nbCaseDeleted = processAPI.deleteArchivedProcessInstancesInAllStates(realId);
        }
        long endTimeDelete = System.currentTimeMillis();
        
        logger.info("MilkPurgeArchivedListPurge.delete: search in "+(endTimeSearch-startTimeSearch)+" ms , delete in "+(endTimeDelete-startTimeDelete)+" ms for "+realId.size() + " Deletion mark="+nbCaseDeleted);
        statistic.countNbItems +=realId.size(); 
        return realId.size();

    }

    /**
     * count the number of line in the 
     * @param outputByte
     * @return
     */
    public long nbLinesInCsv(ByteArrayOutputStream outputByte)
    {
        try
        {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(outputByte.toByteArray())));
        long nbLine=0;
        while (reader.readLine() != null) {
            nbLine++;
        }
        return nbLine;
        }
        catch(Exception e)
        {
            return 0;
        }
        
    }
      
    
}
