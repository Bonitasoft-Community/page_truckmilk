package org.bonitasoft.truckmilk.mapper;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.contract.ContractViolationException;
import org.bonitasoft.engine.bpm.contract.FileInputValue;
import org.bonitasoft.engine.bpm.flownode.FlowNodeExecutionException;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstance;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstanceSearchDescriptor;
import org.bonitasoft.engine.bpm.flownode.UserTaskNotFoundException;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstancesSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ProcessActivationException;
import org.bonitasoft.engine.bpm.process.ProcessDefinitionNotFoundException;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfo;
import org.bonitasoft.engine.bpm.process.ProcessExecutionException;
import org.bonitasoft.engine.bpm.process.ProcessInstance;
import org.bonitasoft.engine.bpm.process.ProcessInstanceNotFoundException;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.exception.UpdateException;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter.FilterProcess;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInToolbox;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.job.MilkJobExecution.ListProcessesResult;
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

    private static String operationPolicyCreateCase = "Create a case";
    private static String operationPolicyExecuteTask = "Execute a task";
    private static String operationPolicyExecuteMessage = "Execute a message";

    private static PlugInParameter cstParamOperationPolicy = PlugInParameter.createInstanceListValues("OperationPolicy",
            "Execution",
            new String[] { operationPolicyCreateCase, operationPolicyExecuteTask, operationPolicyExecuteMessage },
            operationPolicyCreateCase,
            "When a data is detected, which operation? " + operationPolicyCreateCase + ": create a case, "
                    + operationPolicyExecuteTask + ": execute a task, "
                    + operationPolicyExecuteMessage + ": execute a message.");

    // operation Create cases
    private static PlugInParameter cstParamCaseSeparator = PlugInParameter.createInstance("Case creation", "Parameters to specify where the case will be created", TypeParameter.SEPARATOR, null, null)
            .withVisibleCondition("milkJob.parametersvalue[ 'OperationPolicy' ] === 'Create a case'");

    private static String processIndentificationStatic = "Static";
    private static String processIdentificationDynamique = "Dynamique";

    private final static String CSTTASKIDENTIFICATION_CASEIDANDNAME = "Case ID and a Task Name";
    private final static String CSTTASKIDENTIFICATION_TASKID ="Task ID";
            private final static String CSTTASKIDENTIFICATION_TASKGROOVY= "Groovy code, which return a taskId";
    

    private static PlugInParameter cstParamProcessIdentificationMode = PlugInParameter.createInstanceListValues("ProcessIdentification",
            "Process Identification",
            new String[] { processIndentificationStatic, processIdentificationDynamique },
            processIndentificationStatic,
            "How to detect the process to create the case ? " + processIndentificationStatic + ": process is specified. "
                    + processIdentificationDynamique + ": process is determined based on the file name, or a variable")
            .withVisibleCondition("milkJob.parametersvalue[ 'OperationPolicy' ] === 'Create a case'");

    private static PlugInParameter cstParamProcessFilter = PlugInParameter.createInstance("processfilter", "Process name where case will be created", TypeParameter.PROCESSNAME, null, "Give the process name to create case/execute tasks")
            .withVisibleCondition("milkJob.parametersvalue[ 'OperationPolicy' ] === 'Create a case' && milkJob.parametersvalue[ 'ProcessIdentification' ] === 'Static'")
            .withFilterProcess(FilterProcess.ONLYENABLED);
    private static PlugInParameter cstParamProcessIdentification = PlugInParameter.createInstance("processIdentification", "Identify the process name", TypeParameter.STRING, null, "Give the process name. Expected result is <processName>[(<processVariable>)]. Example, \"ScanDirectory\" or \"ScanDirectory(1.4)\" or \"{{processNameInParameter}}\" or \"{{processNameInParameter}}({{processVersionInParameter}})\" ")
            .withVisibleCondition("milkJob.parametersvalue[ 'OperationPolicy' ] === 'Create a case' && milkJob.parametersvalue[ 'ProcessIdentification' ] === 'Dynamique'");

    // operation Execute task
    private static PlugInParameter cstParamTaskSeparator = PlugInParameter.createInstance("Task Execution", "Parameters to specify which tasks will be execution", TypeParameter.SEPARATOR, null, null)
            .withVisibleCondition("milkJob.parametersvalue[ 'OperationPolicy' ] === 'Execute a task'");

    private static PlugInParameter cstParamTaskIdentificationMode = PlugInParameter.createInstanceListValues("TaskIdentification",
            "Task Identification",
            new String[] { CSTTASKIDENTIFICATION_CASEIDANDNAME, CSTTASKIDENTIFICATION_TASKID, CSTTASKIDENTIFICATION_TASKGROOVY },
            CSTTASKIDENTIFICATION_CASEIDANDNAME,
            "How to identify task to execte? " + CSTTASKIDENTIFICATION_CASEIDANDNAME + ": Give a caseId and a TaskName. "
                    + CSTTASKIDENTIFICATION_TASKID + ": give a taskId. "
                    +CSTTASKIDENTIFICATION_TASKGROOVY+": Give a piece of Groovy, who should return a taskId")
      
            .withVisibleCondition("milkJob.parametersvalue[ 'OperationPolicy' ] === 'Execute a task'");

    
    
    private static PlugInParameter cstParamCaseId = PlugInParameter.createInstance("CaseId", "Case ID", TypeParameter.STRING, "", "Give the way to find the caseId. Use {{<dataName>}} to map to a data. Example: \"{{caseId}}\" or \"{{FILENAMEWITHOUTSUFFIX}}\"")
            .withVisibleCondition("milkJob.parametersvalue[ 'OperationPolicy' ] === 'Execute a task' && milkJob.parametersvalue[ 'TaskIdentification' ] === '"+CSTTASKIDENTIFICATION_CASEIDANDNAME+"'");

    private static PlugInParameter cstParamTaskName = PlugInParameter.createInstance("TaskName", "Task Name", TypeParameter.STRING, "", "Give the way to identify the taskname in the process. Use {{<dataName>>}} to map a data. Example : \"Review\" or \"{{task_name}}\"")
            .withVisibleCondition("milkJob.parametersvalue[ 'OperationPolicy' ] === 'Execute a task' && milkJob.parametersvalue[ 'TaskIdentification' ] === '"+CSTTASKIDENTIFICATION_CASEIDANDNAME+"'");

    private static PlugInParameter cstParamTaskId = PlugInParameter.createInstance("TaskId", "Task ID", TypeParameter.STRING, "", "Instead of a caseId + TaskName, you can specify the way to retrieve the taskId. Use {{<dataName>>}} to map a data.")
            .withVisibleCondition("milkJob.parametersvalue[ 'OperationPolicy' ] === 'Execute a task'  && milkJob.parametersvalue[ 'TaskIdentification' ] === '"+CSTTASKIDENTIFICATION_TASKID+"'");

    private static PlugInParameter cstParamSearchTaskGroovy = PlugInParameter.createInstance("SearchTask", "Search Task by a Groovy code.", TypeParameter.TEXT, "", "Give a Groovy code to search the Case Id. Use {{<dataName>>}} to map a data, apiAccessor to access Bonita object.")
            .withVisibleCondition("milkJob.parametersvalue[ 'OperationPolicy' ] === 'Execute a task' && milkJob.parametersvalue[ 'TaskIdentification' ] === '"+CSTTASKIDENTIFICATION_TASKGROOVY+"'");

    // data mapping
    private static PlugInParameter cstParamContract = PlugInParameter.createInstance("Contract", "Bonita Contract value (JSON)", TypeParameter.JSON, "",
            "Give the contract, as JSON. {{}} may be used. Function like INTEGER(), LONG() are allowed : example : INTEGER({{FILENAMENAME}} or INTEGER(12)");
            // + "Operation are Long(<attribut>), Integer(<attribut>), Double(<attribut>), Date(<attribut>,<format>), LocalDate(<attribut>,<format>), LocalDateTime(<attribut>,<format>), OffsetDateTime(<attribut>,<format>), Boolean(<attribut>), File(<attribut>) Example, {\"person\" : { \"age\": {{Integer(age)}}; \"birthday\": \"{{LocalDate('aniversary', 'dd/mm/yyyy')}}\"; \"filename\" : \"{{File(fileName)}}\" }.");

    private static BEvent eventProcessNotFound = new BEvent(Mapper.class.getName(), 1, Level.APPLICATIONERROR,
            "Process Not Found", "The process given (name and version) is not found. Case/task can't be created / executed", "Jobs can't run", "Check parameters");
    private static BEvent eventOneProcessExpected = new BEvent(Mapper.class.getName(), 2, Level.APPLICATIONERROR,
            "One process expected", "Only one process is expected", "Jobs can't run", "Check parameters");

    private static BEvent eventBadJson = new BEvent(Mapper.class.getName(), 3, Level.APPLICATIONERROR,
            "Bad JSON", "The JSON calculated can't be parse", "Jobs can't run", "Check parameters");

    private static BEvent eventMapExpected = new BEvent(Mapper.class.getName(), 4, Level.APPLICATIONERROR,
            "Map Expected", "The result of the JSON must be a MAp (i.e. {}), and the result is different", "Jobs can't run", "Check parameters");

    private static BEvent eventProcessNotActivated = new BEvent(Mapper.class.getName(), 5, Level.APPLICATIONERROR,
            "Process not activated", "Process is not activated, impossible to create a case inside", "Jobs can't run", "Activate the process");

    private static BEvent eventProcessException = new BEvent(Mapper.class.getName(), 6, Level.APPLICATIONERROR,
            "Proces Exception", "An exception arrived during the operation", "Jobs can't run", "Check exception");

    private static BEvent eventProcessContractException = new BEvent(Mapper.class.getName(), 7, Level.APPLICATIONERROR,
            "Contract exception", "The contract is not respected", "Jobs can't run", "Check mapper configuration");

    private static BEvent eventInitalisationException = new BEvent(Mapper.class.getName(), 8, Level.APPLICATIONERROR,
            "Initialisation exception", "Exception during initialisation", "Jobs can't run", "Check mapper configuration");
    
    private static BEvent eventLoadFileError = new BEvent(Mapper.class.getName(), 8, Level.APPLICATIONERROR,
            "Load File error", "A file can't be load in the contract", "File can't be processed to create case/Execute task", "Check exception");
    
    private static BEvent eventTaskSearchError = new BEvent(Mapper.class.getName(), 9, Level.APPLICATIONERROR,
            "Task search error", "Way to find the task failed", "No tasks can be found", "Check parameters");
    
    private static BEvent eventTaskExecutionException = new BEvent(Mapper.class.getName(), 10, Level.APPLICATIONERROR,
            "Task execution error", "An exception arrived during the execution of the task", "Task is not executed", "Check error");
    
    private static BEvent eventTaskNotFound = new BEvent(Mapper.class.getName(), 11, Level.APPLICATIONERROR,
            "Task not found", "Task given can't be found", "Task is not executed", "Check error");
    
    
    
    public enum MAPPER_POLICY {
        CASECREATION, TASKEXECUTION, BOTH
    }

    private MAPPER_POLICY policy;

    public Mapper(MAPPER_POLICY policy) {
        this.policy = policy;
    }

    public void addPlugInParameter(MilkPlugInDescription plugInDescription, String additionnalInformationOnContract) {
        // identify the object to create / apply
        if (policy == MAPPER_POLICY.BOTH) {
            plugInDescription.addParameter(cstParamOperationPolicy);
        }
        if (policy == MAPPER_POLICY.CASECREATION || policy == MAPPER_POLICY.BOTH) {
            plugInDescription.addParameter(cstParamCaseSeparator);
            plugInDescription.addParameter(cstParamProcessIdentificationMode);
            plugInDescription.addParameter(cstParamProcessFilter);
            plugInDescription.addParameter(cstParamProcessIdentification);

        }
        if (policy == MAPPER_POLICY.TASKEXECUTION || policy == MAPPER_POLICY.BOTH) {
            plugInDescription.addParameter(cstParamTaskSeparator);
            plugInDescription.addParameter(cstParamTaskIdentificationMode);
            plugInDescription.addParameter(cstParamCaseId);
            plugInDescription.addParameter(cstParamTaskName);
            plugInDescription.addParameter(cstParamTaskId);
            // Search can be done using the string index
            plugInDescription.addParameter(cstParamSearchTaskGroovy);
        }

        // identify the mapping
        plugInDescription.addParameterSeparator("Data Mapping", "How to fullfill the contract for the execution");
        PlugInParameter paramContractAdditionnalInformation = cstParamContract.clone();
        if (additionnalInformationOnContract !=null)
            paramContractAdditionnalInformation.explanation = paramContractAdditionnalInformation.explanation+"<br>"+additionnalInformationOnContract;
        plugInDescription.addParameter(paramContractAdditionnalInformation);

    }

    ListProcessesResult listProcessResult;

    public List<BEvent> initialisation(MilkJobExecution jobExecution) {
        // Search the process
        List<BEvent> listEvents = new ArrayList<>();

        String operationPolicy = jobExecution.getInputStringParameter(cstParamOperationPolicy);
        if (operationPolicyCreateCase.equals(operationPolicy)) {
            String processIdentificationMode = jobExecution.getInputStringParameter(cstParamProcessIdentificationMode);
            if (processIndentificationStatic.equals(processIdentificationMode)) {
                try {
                    listProcessResult = jobExecution.getInputArrayProcess(cstParamProcessFilter, true, new SearchOptionsBuilder(0, 100), ArchivedProcessInstancesSearchDescriptor.PROCESS_DEFINITION_ID, jobExecution.getApiAccessor().getProcessAPI());
                    listEvents.addAll(listProcessResult.listEvents);
                } catch (SearchException e) {
                    // Nothing to do
                } catch (Exception e) {
                    listEvents.add(new BEvent(eventInitalisationException, e, ""));
                }
                if (!BEventFactory.isError(listEvents)
                        && (listProcessResult.listProcessDeploymentInfo.size() != 1))
                    listEvents.add(new BEvent(eventOneProcessExpected, ""));
            }
        }

        return listEvents;
    }

    
    public class MapperResult {

        public List<BEvent> listEvents = new ArrayList<>();
        public ProcessInstance processInstance;
        public String typeOperation;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* perform Operation */
    /*                                                                                  */
    /* ******************************************************************************** */

    /**
     * Perform the operation
     * 
     * @param jobExecution
     * @param parameters
     * @param attachements
     * @return
     */
    public MapperResult performOperation(MilkJobExecution jobExecution, Map<String, Object> parameters, List<File> attachements) {
        String operationPolicy = jobExecution.getInputStringParameter(cstParamOperationPolicy);
        if (operationPolicyCreateCase.equals(operationPolicy)) {
            return createCase(jobExecution, parameters, attachements);
        } else if (operationPolicyExecuteTask.equals(operationPolicy)) {
            return executeTask(jobExecution, parameters, attachements);
        } else if (operationPolicyExecuteMessage.equals(operationPolicy)) {
            return executeMessage(jobExecution, parameters, attachements);
        }
        return null;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Specific operation */
    /*                                                                                  */
    /* ******************************************************************************** */

    /**
     * @param jobExecution
     * @param parameters
     * @param attachements
     * @return
     */

    public MapperResult createCase(MilkJobExecution jobExecution, Map<String, Object> parameters, List<File> attachements) {
        MapperResult mapperResult = new MapperResult();

        ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
        /**
         * First, found the process
         */
        Long processDefinitionId = null;
        String processIdentificationMode = jobExecution.getInputStringParameter(cstParamProcessIdentificationMode);
        if (processIndentificationStatic.equals(processIdentificationMode))
            processDefinitionId = listProcessResult.listProcessDeploymentInfo.get(0).getProcessId();
        else if (processIdentificationDynamique.equals(processIdentificationMode)) {
            String processIndentificationbrut = jobExecution.getInputStringParameter(cstParamProcessIdentification);
            SearchProcessResult searchResult = searchProcess(processIndentificationbrut, parameters, processAPI);
            mapperResult.listEvents.addAll(searchResult.listEvents);
            processDefinitionId = searchResult.processDefinitionId;
            if (processDefinitionId == null)
                return mapperResult;

        }

        // second, fullfill the contract
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
         * MapEntry "firstName" : "Walter"
         * MapEntry "lastName": "Bates"
         * MapEntry "age" : 25
         * MapEntry Contact :
         * -- MapEntry : "address" : 12,
         * -- MapEntry "city": "Grenoble"
         * MapEntry "cv": "file(c:\temp\adoc.pdf)"
         * )
         * this replacement does not replace the file
         */

        // we expect a MAP as the contract
        if (!(contractJson instanceof Map)) {
            mapperResult.listEvents.add(eventMapExpected);
            return mapperResult;
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> contractJsonMap = (Map<String, Object>) contractJson;

        /**
         * we have to create a map of SERIALISABLE, and then create the fileinputvalue when needed
         */
        Map<String, Serializable> instantiationInputs = transformMapFile(contractJsonMap, parameters, mapperResult);
        if (BEventFactory.isError(mapperResult.listEvents))
            return mapperResult;

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

        try {
            mapperResult.processInstance = processAPI.startProcessWithInputs(processDefinitionId, instantiationInputs);
            mapperResult.typeOperation="Case created";
        } catch (ProcessDefinitionNotFoundException e) {
            mapperResult.listEvents.add(new BEvent(eventProcessNotFound, "processDefinitionId[" + processDefinitionId + "]"));
        } catch (ProcessActivationException e) {
            mapperResult.listEvents.add(new BEvent(eventProcessNotActivated, "processDefinitionId[" + processDefinitionId + "]"));
        } catch (ProcessExecutionException e) {
            mapperResult.listEvents.add(new BEvent(eventProcessException, e, "processDefinitionId[" + processDefinitionId + "]"));
        } catch (ContractViolationException e) {
            mapperResult.listEvents.add(new BEvent(eventProcessContractException, e, "processDefinitionId[" + processDefinitionId + "] contract[" + contract + "]"));
        }

        return mapperResult;
    }

    /**
     * 
     * @param jobExecution
     * @param parameters
     * @param attachements
     * @return
     */
    public MapperResult executeTask(MilkJobExecution jobExecution, Map<String, Object> parameters, List<File> attachements) {
        
        MapperResult mapperResult = new MapperResult();
        String taskIdentification = jobExecution.getInputStringParameter(cstParamTaskIdentificationMode);
        SearchOptionsBuilder sob = new SearchOptionsBuilder(0,10);

        String contextSearchTask=" Policy["+taskIdentification+"]: ";
        
        if (CSTTASKIDENTIFICATION_CASEIDANDNAME.equals(taskIdentification)) {
            String caseId   = PlaceHolder.replacePlaceHolder(parameters, jobExecution.getInputStringParameter(cstParamCaseId));
            String taskName     = PlaceHolder.replacePlaceHolder(parameters, jobExecution.getInputStringParameter(cstParamTaskName));
            contextSearchTask+=" CaseId :["+caseId+"] TaskName["+taskName+"]";
            
            sob.filter(HumanTaskInstanceSearchDescriptor.ROOT_PROCESS_INSTANCE_ID, caseId);
            sob.filter(HumanTaskInstanceSearchDescriptor.NAME, taskName);
        }
        if (CSTTASKIDENTIFICATION_TASKID.equals(taskIdentification)) {
            String taskId = PlaceHolder.replacePlaceHolder(parameters, jobExecution.getInputStringParameter(cstParamTaskId));
            contextSearchTask+=" TaskId :["+taskId+"]";
            
            sob.filter(HumanTaskInstanceSearchDescriptor.PARENT_ACTIVITY_INSTANCE_ID, taskId);
        }
        if (CSTTASKIDENTIFICATION_TASKGROOVY.equals(taskIdentification)) {
            // String taskByGroovy = jobExecution.getInputStringParameter(cstParamSearchTaskGroovy);
            // not yet implemented
            contextSearchTask+=" Not Yet Implemented";
            
            return mapperResult;
        }
        try {

            ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
            SearchResult<HumanTaskInstance> searchResult= processAPI.searchHumanTaskInstances(sob.done());
        if (searchResult.getCount()>0) {
            String contractDefinition = jobExecution.getInputStringParameter(cstParamContract);
            String contract = PlaceHolder.replacePlaceHolder(parameters, contractDefinition);

            /**
             * Contract is something like { "inputData" : { "firstName" : "Walter", "lastName": "Bates", "age" : 25, "cv": "{{FILESOURCE}}"}
             * this replacement does not replace the file
             */
            Object contractJson = JSONValue.parse(contract);
            if (!(contractJson instanceof Map)) {
                mapperResult.listEvents.add(eventMapExpected);
                return mapperResult;
            }
            Map<String, Object> contractJsonMap = (Map<String,Object>) contractJson;
            Map<String, Serializable> executionInputs = transformMapFile(contractJsonMap, parameters, mapperResult);

            try {
                // get the processInstance
                mapperResult.processInstance = processAPI.getProcessInstance( searchResult.getResult().get( 0 ).getRootContainerId());
                processAPI.assignUserTask(searchResult.getResult().get( 0 ).getId(), 1);
                
                processAPI.executeUserTask(searchResult.getResult().get( 0 ).getId(), executionInputs);
                mapperResult.typeOperation="Task executed";
            } catch (FlowNodeExecutionException | UpdateException | ProcessInstanceNotFoundException e ) {
                mapperResult.listEvents.add(new BEvent(eventTaskExecutionException, e, "TaskId[" + searchResult.getResult().get( 0 ).getId() + "] contract[" + contract + "] "+e.getMessage()));
            } catch (ContractViolationException e) {
                mapperResult.listEvents.add(new BEvent(eventProcessContractException, e, "TaskId[" + searchResult.getResult().get( 0 ).getId() + "] contract[" + contract + "]"));
            } catch (UserTaskNotFoundException e) {
                mapperResult.listEvents.add(new BEvent(eventTaskNotFound, e, "TaskId[" + searchResult.getResult().get( 0 ).getId() + "] contract[" + contract + "]"));
            }
        }
        else {
            // not found, do nothing for the moment with the source
            
        }
        } catch (SearchException e) {
            mapperResult.listEvents.add( new BEvent(eventTaskSearchError, contextSearchTask));
       
        }

        
        return mapperResult;
    }

    public MapperResult executeMessage(MilkJobExecution jobExecution, Map<String, Object> parameters, List<File> attachements) {
        MapperResult mapperResult = new MapperResult();
        return mapperResult;
    }

    /**
     * recursive call to transform all the map AND to place the FileInputValue
     * 
     * @param contractJsonMap
     * @param parameters
     * @return
     */
    private Map<String, Serializable> transformMapFile(Map<String, Object> contractJsonMap, Map<String, Object> parameters, MapperResult mapperResult) {
        Map<String, Serializable> instantiationInputs = new HashMap<>();
        for (String key : contractJsonMap.keySet()) {
            instantiationInputs.put(key, (Serializable) contractJsonMap.get(key));
        }
        mapperResult.listEvents.addAll( resolveFonction( instantiationInputs, parameters) );
        
        return instantiationInputs;
    }
    private List<BEvent> resolveFonction( Map<String, Serializable> instantiationInputs, Map<String, Object> parameters) {
        List<BEvent> listEvents  = new ArrayList<>();
        List<String> listKeys = new ArrayList<>();
        for (String key : instantiationInputs.keySet())
            listKeys.add( key );
        
        for (String key : listKeys) {
            if (instantiationInputs.get( key ) instanceof Map)
                listEvents.addAll( resolveFonction( (Map<String,Serializable>) instantiationInputs.get( key ), parameters));
            else if (instantiationInputs.get( key ).toString().toUpperCase().startsWith("FILE(")) {
                // replace the file by a documentEntry
                String fileName=instantiationInputs.get( key ).toString().substring("FILE(".length());
                fileName = fileName.substring(0,fileName.length()-1);
                try {
                    File file = new File(fileName); 
                    // instantiationInputs.put(key, file);
                    
                    Path path = Paths.get(file.getAbsolutePath());
                    byte[] fileContent = Files.readAllBytes(path);
                    String mimeType = Files.probeContentType(path);
                    FileInputValue valueTranslated = new FileInputValue(fileName, mimeType, fileContent);
                    instantiationInputs.put(key,valueTranslated);
                } catch (Exception e) {
                    listEvents.add( new BEvent(eventLoadFileError, e, "File ["+fileName+"]"));
                }
                
            } else if (instantiationInputs.get( key ).toString().toUpperCase().startsWith("INTEGER(")) {
                String value=instantiationInputs.get( key ).toString().substring("INTEGER(".length());
                value = value.substring(0,value.length()-1);
                try {
                    Integer intValue = Integer.valueOf( value ); 
                    instantiationInputs.put(key,intValue);
                } catch(Exception e) {
                    instantiationInputs.put(key,value);
                }
            }
        }
        return listEvents;
    }

    public class SearchProcessResult {

        public List<BEvent> listEvents = new ArrayList<>();
        public Long processDefinitionId;
    }

    /**
     * Search a specific process
     * 
     * @param processNameVersionBrut
     * @param parameters
     * @param processAPI
     * @return
     */
    public SearchProcessResult searchProcess(String processNameVersionBrut, Map<String, Object> parameters, ProcessAPI processAPI) {
        SearchProcessResult searchProcessResult = new SearchProcessResult();
        String processNameVersion = PlaceHolder.replacePlaceHolder(parameters, processNameVersionBrut);
        SearchResult<ProcessDeploymentInfo> searchResult;
        try {
            searchResult = MilkPlugInToolbox.getListProcessDefinitionId(processNameVersion, FilterProcess.ONLYENABLED, processAPI);
            if (searchResult.getCount() > 0) {
                searchProcessResult.processDefinitionId = searchResult.getResult().get(0).getProcessId();
                return searchProcessResult;
            }
        } catch (SearchException e) {
        }

        searchProcessResult.listEvents.add(new BEvent(eventProcessNotFound, "Process[" + processNameVersionBrut + "] - [" + processNameVersion + "]"));
        return searchProcessResult;
    }

}
