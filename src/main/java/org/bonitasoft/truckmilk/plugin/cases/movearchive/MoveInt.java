package org.bonitasoft.truckmilk.plugin.cases.movearchive;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.api.IdentityAPI;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstance;
import org.bonitasoft.engine.identity.User;
import org.bonitasoft.engine.identity.UserNotFoundException;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.properties.DatabaseConnection;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.plugin.cases.MilkMoveArchiveCases.MoveParameters;

public abstract class MoveInt {
    
    private final static String LOGGER_LABEL = "MilkMoveArchive.MoveInt";
    private final static Logger logger = Logger.getLogger(MoveInt.class.getName());
  
    protected final static BEvent eventCantExecuteExistProcessInstance = new BEvent(MoveInt.class.getName(), 1, Level.ERROR,
            "Can't count number of process Instance", "A count is executed to verify if the process instance was already copied, and it failed", "ProcessInstance are considered as already copied", "Check the exception");
    protected final static BEvent eventSaveRecordFailed = new BEvent(MoveInt.class.getName(), 2, Level.ERROR,
            "Insert record failed", "Insert in the external database failed", "Case is not moved", "Check the exception");

    protected final static BEvent eventCantAccessDocument = new BEvent(MoveInt.class.getName(), 3, Level.ERROR,
            "Can't access Document", "Can't access a document, move will be incomplete", "Case is not moved", "Check the exception");

    protected final static BEvent eventProcessDefinitionNotFound = new BEvent(MoveInt.class.getName(), 4, Level.ERROR,
            "ProcessDefinition not found", "The process definition is not found", "Case can't be moved", "Check process definition and the database");

    public class ResultMove {

        public List<BEvent> listEvents = new ArrayList<>();
        public int nbActivities = 0;
        public int nbDatas = 0;
        public int nbDocuments = 0;
        public int nbComments = 0;
        public int nbProcessInstances=0;

    }
    
    public abstract List<BEvent> checkJobEnvironment(MilkJobExecution milkJobExecution, DatabaseConnection.ConnectionResult con);
        
        
    public abstract ResultMove existProcessInstanceInArchive(long processInstance, long tenantId, Connection con);

    public abstract List<BEvent> copyToDatabase(MoveParameters moveParameters, ArchivedProcessInstance archivedProcessInstance, APIAccessor apiAccessor, long tenantId, MilkJobOutput milkJobOutput, Connection con, Map<Long, User> cacheUsers);
        
    protected Long dateToLong(Date date) {
        if (date == null)
            return null;
        return date.getTime();
    }
    protected String getUserName(Long userId, IdentityAPI identityAPI, Map<Long, User> cacheUsers) {
        if (userId == null || userId < 0)
            return null;
        User user = cacheUsers.get(userId);
        if (user != null)
            return user.getUserName();
        try {
            user = identityAPI.getUser(userId);
        } catch (UserNotFoundException e) {
            // user does not exist? strange, because the user come from the ProcessInstance
            logger.severe(LOGGER_LABEL + " UserId not found[" + userId + "]");
            return null;
        }
        cacheUsers.put(userId, user);
        return user.getUserName();
    }
}
