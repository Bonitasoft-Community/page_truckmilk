package org.bonitasoft.truckmilk.mapper;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.contract.ContractViolationException;
import org.bonitasoft.engine.bpm.process.ProcessActivationException;
import org.bonitasoft.engine.bpm.process.ProcessDefinitionNotFoundException;
import org.bonitasoft.engine.bpm.process.ProcessExecutionException;
import org.bonitasoft.engine.bpm.process.ProcessInstance;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.PlaceHolder;
import org.json.simple.JSONValue;

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

    private static PlugInParameter cstParamProcessName = PlugInParameter.createInstance("ProcessName", "Process name", TypeParameter.PROCESSNAME, "", "Give the process name to create case/execute tasks");
    private static PlugInParameter cstParamCaseId = PlugInParameter.createInstance("CaseId", "Case ID", TypeParameter.STRING, "", "");
    private static PlugInParameter cstParamTaskId = PlugInParameter.createInstance("TaskId", "Task ID", TypeParameter.STRING, "", "");
    private static PlugInParameter cstParamTaskName = PlugInParameter.createInstance("TaskName", "Task Name", TypeParameter.STRING, "", "");
    private static PlugInParameter cstParamStringIndex1 = PlugInParameter.createInstance("StringIndex1", "Search by the String Index 1", TypeParameter.STRING, "", "Give a key to search the task by a string index. {{value}} can be used, example {{fileName}}");
    private static PlugInParameter cstParamStringIndex2 = PlugInParameter.createInstance("StringIndex2", "Search by the String Index 2", TypeParameter.STRING, "", "Give a key to search the task by a string index. {{value}} can be used, example {{fileName}}");
    private static PlugInParameter cstParamStringIndex3 = PlugInParameter.createInstance("StringIndex3", "Search by the String Index 3", TypeParameter.STRING, "", "Give a key to search the task by a string index. {{value}} can be used, example {{fileName}}");
    private static PlugInParameter cstParamStringIndex4 = PlugInParameter.createInstance("StringIndex4", "Search by the String Index 4", TypeParameter.STRING, "", "Give a key to search the task by a string index. {{value}} can be used, example {{fileName}}");
    private static PlugInParameter cstParamStringIndex5 = PlugInParameter.createInstance("StringIndex5", "Search by the String Index 5", TypeParameter.STRING, "", "Give a key to search the task by a string index. {{value}} can be used, example {{fileName}}");
    private static PlugInParameter cstParamContract = PlugInParameter.createInstance("Contract", "Bonita Contract value (JSON)", TypeParameter.JSON, "", "Give the contract, as JSON. {{}} may be used. Operation are Long(<attribut>), Integer(<attribut>), Double(<attribut>), Date(<attribut>,<format>), LocalDate(<attribut>,<format>), LocalDateTime(<attribut>,<format>), OffsetDateTime(<attribut>,<format>), Boolean(<attribut>), File(<attribut>) Example, 'person' : { 'age': {{Integer(age)}}; 'birtday': '{{LocalDate(aniversary', 'dd/mm/yyyy')}}; 'filename' : '{{File(fileName)}}' }.");
    

    private static BEvent eventProcessNotFound = new BEvent(Mapper.class.getName(), 1, Level.APPLICATIONERROR,
            "Process Not Found", "The process given (name and version) is not found. Case/task can't be created / executed", "Jobs can't run", "Check parameters");

    private static BEvent EVENT_BAD_JSON = new BEvent(Mapper.class.getName(), 2, Level.APPLICATIONERROR,
            "Bad JSON", "The JSON calculated can't be parse", "Jobs can't run", "Check parameters");

    private static BEvent EVENT_MAP_EXPECTED = new BEvent(Mapper.class.getName(), 3, Level.APPLICATIONERROR,
            "Map Expected", "The result of the JSON must be a MAp (i.e. {}), and the result is different", "Jobs can't run", "Check parameters");

    private static BEvent EVENT_PROCESS_NOT_ACTIVATED = new BEvent(Mapper.class.getName(), 4, Level.APPLICATIONERROR,
            "Process not activated", "Process is not activated, impossible to create a case inside", "Jobs can't run", "Activate the process");

    private static BEvent EVENT_PROCESS_EXCEPTION = new BEvent(Mapper.class.getName(), 5, Level.APPLICATIONERROR,
            "Proces Exception", "An exception arrived during the operation", "Jobs can't run", "Check exception");

    private static BEvent EVENT_PROCESS_CONTRACT_EXCEPTION = new BEvent(Mapper.class.getName(), 6, Level.APPLICATIONERROR,
            "Contract exception", "The contract is not respected", "Jobs can't run", "Check mapper configuration");

    public enum MAPPER_POLICY {
        CASECREATION, TASKEXECUTION
    };

    private MAPPER_POLICY policy;

    public Mapper(MAPPER_POLICY policy) {
        this.policy = policy;
    }

    public void addPlugInParameter(MilkPlugInDescription plugInDescription) {
        // identify the object to create / apply
        if (policy == MAPPER_POLICY.CASECREATION) {
            plugInDescription.addParameter(cstParamProcessName);
        } else {
            plugInDescription.addParameter(cstParamCaseId);
            plugInDescription.addParameter(cstParamTaskId);
            plugInDescription.addParameter(cstParamTaskName);
            // Search can be done using the string index
            plugInDescription.addParameter(cstParamStringIndex1);
            plugInDescription.addParameter(cstParamStringIndex2);
            plugInDescription.addParameter(cstParamStringIndex3);
            plugInDescription.addParameter(cstParamStringIndex4);
            plugInDescription.addParameter(cstParamStringIndex5);
        }

        // identify the mapping
        plugInDescription.addParameter(cstParamContract);

    }

    String processName = null;
    String processVersion = null;

    Long processDefinitionId;

    public List<BEvent> initialisation(MilkJobExecution jobExecution, APIAccessor apiAccessor) {
        // Search the process
        List<BEvent> listEvents = new ArrayList<>();
        ProcessAPI processAPI = apiAccessor.getProcessAPI();
        processName = jobExecution.getInputStringParameter(cstParamProcessName);
        
        try {
            processDefinitionId = processAPI.getProcessDefinitionId(processName, processVersion);
        } catch (ProcessDefinitionNotFoundException e) {
            listEvents.add(new BEvent(eventProcessNotFound, "ProcessName[" + processName + "] version[" + processVersion + "]"));
        }

        return listEvents;
    }

    public void searchProcess() {

    }

    public void searchTask() {

    }

    public class MapperResult {

        List<BEvent> listEvents = new ArrayList<BEvent>();
        ProcessInstance processInstance;
    }

    public MapperResult createCase(MilkJobExecution jobExecution, Map<String, Object> parameters, List<File> attachements, APIAccessor apiAccessor) {
        MapperResult mapperResult = new MapperResult();
        String contractDefinition = jobExecution.getInputStringParameter(cstParamContract);

        /**
         * Contract Definition is something like { "inputData" : { "firstName" : "{{myFirstname}}", "lastName": "{{myLastName}}", "age" : {{age}}, "cv":
         * "{{FILESOURCE}}" }
         */
        String contract = PlaceHolder.replacePlaceHolder(parameters, contractDefinition);

        /**
         * Contract is something like { "inputData" : { "firstName" : "Walter", "lastName": "Bates", "age" : 25, "cv": "{{FILESOURCE}}"}
         * this replacement does not replace the file
         */

        /**
         * Time to create the object
         */

        Object contractJson = JSONValue.parse(contract);
        /**
         * ContractJson is something like
         * Map("inputDate") :
         * Map ("firstName","Walter"
         * "lastName": "Bates",
         * "age" : 25,
         * "cv": "{{FILESOURCE}}"
         * )
         * this replacement does not replace the file
         */

        // we expect a MAP as the contract
        if (!(contractJson instanceof Map)) {
            mapperResult.listEvents.add(EVENT_MAP_EXPECTED);
            return mapperResult;
        }

        Map<String, Object> contractJsonMap = (Map) contractJson;

        /**
         * we have to create a map of SERIALISABLE, and then create the fileinputvalue when needed
         */
        Map<String, Serializable> instantiationInputs = transformMapFile(contractJsonMap, parameters);

        /**
         * ContractJson is something like
         * Map("inputDate") :
         * Map ("firstName","Walter"
         * "lastName": "Bates",
         * "age" : 25,
         * "cv": FileInputValue()
         * )
         * this replacement does not replace the file
         */

        /**
         * we get now something
         */

        ProcessAPI processAPI = apiAccessor.getProcessAPI();
        try {
            mapperResult.processInstance = processAPI.startProcessWithInputs(processDefinitionId, instantiationInputs);
        } catch (ProcessDefinitionNotFoundException e) {
            mapperResult.listEvents.add(new BEvent(eventProcessNotFound, "ProcessName[" + processName + "] version[" + processVersion + "]"));
        } catch (ProcessActivationException e) {
            mapperResult.listEvents.add(new BEvent(EVENT_PROCESS_NOT_ACTIVATED, "ProcessName[" + processName + "] version[" + processVersion + "]"));
        } catch (ProcessExecutionException e) {
            mapperResult.listEvents.add(new BEvent(EVENT_PROCESS_EXCEPTION, "ProcessName[" + processName + "] version[" + processVersion + "]"));
        } catch (ContractViolationException e) {
            mapperResult.listEvents.add(new BEvent(EVENT_PROCESS_CONTRACT_EXCEPTION, "ProcessName[" + processName + "] version[" + processVersion + "]"));
        }

        return mapperResult;
    }

    /**
     * recursive call to transform all the map AND to place the FileInputValue 
     * 
     * @param contractJsonMap
     * @param parameters
     * @return
     */
    private Map<String, Serializable> transformMapFile(Map<String, Object> contractJsonMap, Map<String, Object> parameters) {
        Map<String, Serializable> instantiationInputs = new HashMap<String, Serializable>();
        for (String key : contractJsonMap.keySet()) {
            instantiationInputs.put(key, (Serializable) contractJsonMap.get(key));
        }
        return instantiationInputs;
    }
}
