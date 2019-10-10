package org.bonitasoft.truckmilk.mapper;

import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;

/* ******************************************************************************** */
/*                                                                                  */
/* Mapper */
/*                                                                                  */
/* This class describe a list of service to allow the mapping between external */
/*
 * information, and a process or a task.
 * principle is :
 * From external information (directory, email, pooling on an external service)
 * I want to create a case, or to execute a task
 * Create a case:
 * Process Name and Process Version has be given
 * Input data has to be given
 * Execute a task
 * CaseId has to be found
 * Task name or taskId has to be found
 * The service need to identify information. For example, to create a case,
 * the process name has to be given.
 * Idea is to give a list of AFFECTATION. A affectation has a **variable**, and an **expression**
 * PROCESSNAME = {{extract(filename,12,10)}}
 * According the operation, a set of variable has to be defined
 * - create a case: PROCESSNAME, PROCESSVERSION, SOURCEOBJECT, INPUT.<name>
 * - execute a task : PROCESSNAME, PROCESSVERSION, CASEID, STRINGINDEX1, STRINGINDEX2, INPUT.<name>
 * According the source (monitor a directory, receive and email), an input dictionary is provided.
 * For example,
 * - directory monitoring: the inputdictionnary contains the directory path, the file name.
 * - CSV monitoring : inputdictionary contains directory path, filename, one attribut per CSV
 * attribute
 * - email : inputdictionnary contains emailsubject, emailcontent, attachment file, emailfrom,
 * emailto
 * Then the AFFECTATION can build the expression using the inputdictionary, and some function:
 * - substring( <source>, indexFrom, size)
 * - find( <soure>, <searchPattern>, <endPattern)
 * - trim( <source>)
 * Example to create a case
 * PROCESSNAME=CreateCase or PROCESSNAME={{emailsubject}} or PROCESSNAME={{extract(emailsubject,
 * 10,5)}}
 * INPUT.InvoiceBuild={{attachement1}}
 * INPUT.dateOfInvoice={{find(emailcontent,"date Of Invoice", "\n");}}
 * INPUT.userName={{emailfrom}}
 * INPUT.source=Email
 * INPUT.description=Email from {{emailfrom}} received the {{emaildate}}
 * ==> We don't give the process version, assuming the last version
 * Example to execute a task
 * TASKID={{find(emailcontent,"TASKID:","\n")}} or STRINGINDEX1={{emailsubject}}
 * INPUT.ExpenseNoteComment={{attachement1}}
 * INPUT.Comment={{emailcontent}}
 */
/* ******************************************************************************** */

public class Mapper {

    private static PlugInParameter cstParamProcessName = PlugInParameter.createInstance("ProcessName", "Process name", TypeParameter.STRING, "", "Give the process name to create case / execute tasks");
    private static PlugInParameter cstParamProcessVersion = PlugInParameter.createInstance("ProcessVersion", "Process version", TypeParameter.STRING, "", "");
    private static PlugInParameter cstParamCaseId = PlugInParameter.createInstance("CaseId", "Case ID", TypeParameter.STRING, "", "");
    private static PlugInParameter cstParamTaskId = PlugInParameter.createInstance("TaskId", "Task ID", TypeParameter.STRING, "", "");
    private static PlugInParameter cstParamTaskName = PlugInParameter.createInstance("TaskName", "Task Name", TypeParameter.STRING, "", "");
    private static PlugInParameter cstParamStringIndex1 = PlugInParameter.createInstance("StringIndex1", "String Index 1", TypeParameter.STRING, "", "");
    private static PlugInParameter cstParamStringIndex2 = PlugInParameter.createInstance("StringIndex2", "String Index 2", TypeParameter.STRING, "", "");
    private static PlugInParameter cstParamStringIndex3 = PlugInParameter.createInstance("StringIndex3", "String Index 3", TypeParameter.STRING, "", "");
    private static PlugInParameter cstParamStringIndex4 = PlugInParameter.createInstance("StringIndex4", "String Index 4", TypeParameter.STRING, "", "");
    private static PlugInParameter cstParamStringIndex5 = PlugInParameter.createInstance("StringIndex5", "String Index 5", TypeParameter.STRING, "", "");

    public static void addPlugInParameter(PlugInDescription plugInDescription) {
        plugInDescription.addParameter(cstParamProcessName);
        plugInDescription.addParameter(cstParamProcessVersion);
        plugInDescription.addParameter(cstParamCaseId);
        plugInDescription.addParameter(cstParamTaskId);
        plugInDescription.addParameter(cstParamTaskName);
        plugInDescription.addParameter(cstParamStringIndex1);
        plugInDescription.addParameter(cstParamStringIndex2);
        plugInDescription.addParameter(cstParamStringIndex3);
        plugInDescription.addParameter(cstParamStringIndex4);
        plugInDescription.addParameter(cstParamStringIndex5);

    }

    public void searchProcess() {

    }

    public void searchTask() {

    }

    public void createCase() {

    }

}
