import java.lang.management.RuntimeMXBean;
import java.lang.management.ManagementFactory;

import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.logging.Logger;
import java.io.File
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.lang.Runtime;

import org.json.simple.JSONObject;
import org.codehaus.groovy.tools.shell.CommandAlias;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;



import javax.naming.Context;
import javax.naming.InitialContext;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import javax.sql.DataSource;
import java.sql.DatabaseMetaData;
import java.sql.Clob;
import java.util.Date;

import org.apache.commons.lang3.StringEscapeUtils


import org.bonitasoft.engine.identity.User;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.console.common.server.page.PageContext
import org.bonitasoft.console.common.server.page.PageController
import org.bonitasoft.console.common.server.page.PageResourceProvider
import org.bonitasoft.engine.exception.AlreadyExistsException;
import org.bonitasoft.engine.exception.BonitaHomeNotSetException;
import org.bonitasoft.engine.exception.CreationException;
import org.bonitasoft.engine.exception.DeletionException;
import org.bonitasoft.engine.exception.ServerAPIException;
import org.bonitasoft.engine.exception.UnknownAPITypeException;
import org.bonitasoft.engine.bpm.process.ProcessDefinitionNotFoundException;

import org.bonitasoft.engine.api.TenantAPIAccessor;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstance;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstanceSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstance;
import org.bonitasoft.engine.bpm.process.ArchivedProcessInstancesSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ProcessInstance;
import org.bonitasoft.engine.bpm.process.ProcessInstanceSearchDescriptor;
import org.bonitasoft.engine.session.APISession;
import org.bonitasoft.engine.api.ProcessAPI;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.log.event.BEvent.Level;

import org.bonitasoft.truckmilk.MilkAccessAPI;
import org.bonitasoft.truckmilk.MilkAccessAPI.Parameter;


public class Actions {

    private static Logger logger= Logger.getLogger("org.bonitasoft.custompage.truckmilk.groovy");
    
    
        
    
      // 2018-03-08T00:19:15.04Z
    public final static SimpleDateFormat sdfJson = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    public final static SimpleDateFormat sdfHuman = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* doAction */
    /*                                                                      */
    /* -------------------------------------------------------------------- */

    public static Index.ActionAnswer doAction(HttpServletRequest request, String paramJsonSt, HttpServletResponse response, PageResourceProvider pageResourceProvider, PageContext pageContext) {
                
        // logger.info("#### cockpit:Actions start");
        Index.ActionAnswer actionAnswer = new Index.ActionAnswer(); 
        List<BEvent> listEvents=new ArrayList<BEvent>();
        
        try {
            String action=request.getParameter("action");
            
            if (action==null || action.length()==0 )
            {
                actionAnswer.isManaged=false;
                logger.info("#### log:Actions END No Actions");
                return actionAnswer;
            }
            actionAnswer.isManaged=true;
            
            APISession apiSession = pageContext.getApiSession();
            HttpSession httpSession = request.getSession();            
            ProcessAPI processAPI = TenantAPIAccessor.getProcessAPI(apiSession);

            Parameter parameter = Parameter.getInstanceFromJson(paramJsonSt);
            
            parameter.apiSession=apiSession;
            parameter.commandAPI = TenantAPIAccessor.getCommandAPI( apiSession );
            parameter.platFormAPI= null; // TenantAPIAccessor.getPlatformAPI( apiSession );
            parameter.pageDirectory = pageResourceProvider.getPageDirectory();
            MilkAccessAPI milkAccessAPI = MilkAccessAPI.getInstance();

            logger.info("#### log:Actions_2 ["+action+"]");
            if ("init".equals(action))
            {
               actionAnswer.responseMap = milkAccessAPI.startup( parameter);
            }
            else if ("refresh".equals(action))
            {
               actionAnswer.responseMap = milkAccessAPI.getRefreshInformation( parameter);
            }
            
            else if ("addTour".equals(action))
            {
               actionAnswer.responseMap = milkAccessAPI.addTour( parameter);                
            }          
            else if ("removeTour".equals(action))
            {
               actionAnswer.responseMap = milkAccessAPI.removeTour( parameter);                
            } 
            else if ("startTour".equals(action))
            {
               actionAnswer.responseMap = milkAccessAPI.startTour( parameter);                
            } 
            else if ("stopTour".equals(action))
            {
               actionAnswer.responseMap = milkAccessAPI.stopTour( parameter);                
            }              
            else if ("updateTour".equals(action))
            {
               actionAnswer.responseMap = milkAccessAPI.updateTour( parameter);                
            } 
            else if ("immediateExecution".equals(action))
            {
                logger.info("#### log:Actions call immediateExecution");
                
               actionAnswer.responseMap = milkAccessAPI.immediateExecution( parameter);
               logger.info("#### log:Actions call immediateExecution : YES");

            } 
            
            else  if ("scheduler".equals(action))
            {
                actionAnswer.responseMap = milkAccessAPI.scheduler( parameter);                
             }             
            else  if ("schedulermaintenance".equals(action))
            {
                actionAnswer.responseMap = milkAccessAPI.schedulerMaintenance( parameter);                
             } 
            logger.info("#### log:Actions END responseMap ="+actionAnswer.responseMap.size());
            return actionAnswer;
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            logger.severe("#### log:Groovy Exception ["+e.toString()+"] at "+exceptionDetails);
            actionAnswer.isResponseMap=true;
            actionAnswer.responseMap.put("Error", "log:Groovy Exception ["+e.toString()+"] at "+exceptionDetails);
            

            
            return actionAnswer;
        }
    }

    
    
    
    
}
