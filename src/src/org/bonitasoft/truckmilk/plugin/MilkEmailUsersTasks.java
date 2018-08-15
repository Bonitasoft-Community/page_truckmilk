package org.bonitasoft.truckmilk.plugin;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

import javax.mail.*;
import javax.mail.internet.*;
import javax.activation.*;

import org.bonitasoft.engine.api.APIAccessor;
import org.bonitasoft.engine.api.IdentityAPI;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.api.ProfileAPI;
import org.bonitasoft.engine.bpm.flownode.ActivityInstanceCriterion;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstance;
import org.bonitasoft.engine.identity.ContactData;
import org.bonitasoft.engine.identity.User;
import org.bonitasoft.engine.identity.UserCriterion;
import org.bonitasoft.engine.identity.UserSearchDescriptor;
import org.bonitasoft.engine.profile.Profile;
import org.bonitasoft.engine.profile.ProfileMember;
import org.bonitasoft.engine.profile.ProfileMemberSearchDescriptor;
import org.bonitasoft.engine.profile.ProfileSearchDescriptor;
import org.bonitasoft.engine.search.SearchOptions;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.engine.search.descriptor.SearchProfileMemberUserDescriptor;
import org.bonitasoft.engine.search.descriptor.SearchUserDescriptor;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugInDescription;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugTourInput;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugTourOutput;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.tour.MilkPlugInTour;


public class MilkEmailUsersTasks extends MilkPlugIn{

    public static Logger logger = Logger.getLogger(MilkEmailUsersTasks.class.getName());

    private static BEvent EVENT_SEND_EMAIL = new BEvent(MilkEmailUsersTasks.class.getName(), 1, Level.APPLICATIONERROR,
            "Mail send failed", "Email can't be send", "No email send", "Check the error");

    
    private static BEvent EVENT_OPERATION_SUCCESS = new BEvent(MilkEmailUsersTasks.class.getName(), 2, Level.SUCCESS,
            "Mails was sent if needed", "Mails was sent");
    
    private static BEvent EVENT_OPERATION_ERROR = new BEvent(MilkEmailUsersTasks.class.getName(), 3, Level.APPLICATIONERROR,
            "Error during sending", "Errors arrived : no email in users, smtp server down", "Some emails failed", "Check errors");

    private static BEvent EVENT_PROFILES_DOES_NOT_EXIST = new BEvent(MilkEmailUsersTasks.class.getName(), 4, Level.APPLICATIONERROR,
        "Profiles does not exist", "The profiles does not exist", "Operation failed", "Give an existing profile name");


    private static PlugInParameter cstParamSmtpUseSSL       = PlugInParameter.createInstance("smtpusessl", TypeParameter.BOOLEAN, Boolean.FALSE, null);
    private static PlugInParameter cstParamSmtpStartTLS     = PlugInParameter.createInstance("smtpstarttls", TypeParameter.BOOLEAN, Boolean.FALSE, null);
    
    private static PlugInParameter cstParamProfilesUser     = PlugInParameter.createInstance("profilesuser", TypeParameter.ARRAY, null, null);

    private static PlugInParameter cstParamEmailSmtpHost    = PlugInParameter.createInstance("smtphost", TypeParameter.STRING, "smtp.gmail.com", "Smtp host name");
    private static PlugInParameter cstParamEmailSmtpPort    = PlugInParameter.createInstance("smtpport", TypeParameter.LONG, 465L, "Smtp port");
    private static PlugInParameter cstParamSmtpSSLTrust     = PlugInParameter.createInstance("smtpssltrust", TypeParameter.BOOLEAN, "", null);
    
    
    private static PlugInParameter cstParamSmtpUserName     = PlugInParameter.createInstance("smtpusername", TypeParameter.STRING, null, "If you server require an authentication, give the login/password");
    private static PlugInParameter cstParamSmtpUserPassword = PlugInParameter.createInstance("smtpuserpassword", TypeParameter.STRING, null, "If you server require an authentication, give the login/password");
 
    private static PlugInParameter cstParamEmailFrom = PlugInParameter.createInstance("emailfrom", TypeParameter.STRING, "bonitasoftreminder@myserver.com", "The 'mail from' attribute");
    private static PlugInParameter cstParamEmailSubject= PlugInParameter.createInstance("emailsubject", TypeParameter.STRING, "You have some pending tasks", "Subject on the email");
    private static PlugInParameter cstParamEmailText= PlugInParameter.createInstance("emailtext", TypeParameter.TEXT, "Your assigned task <br>{{assignedtasks}}<p>Your pending tasks:<br>{{pendingtasks}}", "Content of the email. Use the place holder {{assignedtasks}} and {{pendingtasks}}");

    private static PlugInParameter cstParamMaxTaskInEmail= PlugInParameter.createInstance("maxtasksinemail", TypeParameter.LONG, 30L, "Number of task to display in the email");
    
    private static PlugInParameter cstParamBonitaHost = PlugInParameter.createInstance("bonitahost", TypeParameter.STRING, null, "In the Email, the HTTP link will use this information");
    private static PlugInParameter cstParamBonitaPort = PlugInParameter.createInstance("bonitaport", TypeParameter.LONG, null, "In the Email, the HTTP link will use this information");
    
    
    /**
     * 
     * it's embedded
     */
    public boolean isEmbeded() {
        return true;
    };

    @Override
    public PlugInDescription getDescription() {
        PlugInDescription plugInDescription = new PlugInDescription();
        plugInDescription.name = "EmailUsersTasks";
        plugInDescription.displayName = "Emails users tasks";
        plugInDescription.description = "For all users part of the given profile(s), an email is send with a link on all pending tasks";
        plugInDescription.addParameter(cstParamProfilesUser);
        plugInDescription.addParameter(cstParamSmtpUseSSL);
        plugInDescription.addParameter(cstParamSmtpStartTLS);
        
        plugInDescription.addParameter(cstParamEmailSmtpHost);
        plugInDescription.addParameter(cstParamEmailSmtpPort);
        plugInDescription.addParameter(cstParamSmtpSSLTrust);
        plugInDescription.addParameter(cstParamSmtpUserName);
        plugInDescription.addParameter(cstParamSmtpUserPassword);
        plugInDescription.addParameter(cstParamMaxTaskInEmail);
        plugInDescription.addParameter(cstParamEmailText);
        
        
        return plugInDescription;
    }
    @Override
    public PlugTourOutput execute(PlugTourInput input, APIAccessor apiAccessor) {
        PlugTourOutput plugTourOutput= input.getPlugTourOutput();
        
        // read profiles
        List<String> listProfilesName = (List<String>) input.getInputListParameter(cstParamProfilesUser);
        if (listProfilesName.size() ==0)
        {
            plugTourOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
            return plugTourOutput;
        }
        try
        {
        ProfileAPI profileAPI = apiAccessor.getProfileAPI();
        ProcessAPI processAPI = apiAccessor.getProcessAPI();
        IdentityAPI identityAPI = apiAccessor.getIdentityAPI();
        // collect users in profiles
        Set<Long> userIdToSendEmail = new HashSet<Long>();
        List<Long> listProfileId = new ArrayList<Long>();
        for (int i=0;i<listProfilesName.size();i++)
        {
          SearchOptionsBuilder searchOptionsBuilder =  new SearchOptionsBuilder(0,10000);
          
            searchOptionsBuilder.filter( ProfileSearchDescriptor.NAME, listProfilesName.get( i ));
            SearchResult<Profile> searchResultProfiles = profileAPI.searchProfiles(searchOptionsBuilder.done());
            if (searchResultProfiles.getCount()==0)
            {
              plugTourOutput.addEvent( new BEvent(EVENT_PROFILES_DOES_NOT_EXIST, "profile["+listProfilesName.get( i )));
            }
            else
            {
              for (Profile profile : searchResultProfiles.getResult())
              {
                listProfileId.add( profile.getId());
              }
            }
        }
        
        if (listProfileId.size()==0)
        {
          plugTourOutput.executionStatus = ExecutionStatus.WARNING;
          return plugTourOutput;
        }
        //---------------- then search each profiles member
        SearchOptionsBuilder searchOptionsBuilder =  new SearchOptionsBuilder(0,10000);
        for (int i=0;i<listProfileId.size();i++)
        {
            if (i>0)
                searchOptionsBuilder.or();
            searchOptionsBuilder.filter( ProfileMemberSearchDescriptor.PROFILE_ID, listProfileId.get( i ));
        }
    
        String[] listMembersType = new String[] { "user", "role", "group", "roleAndGroup" };
        for (String memberType : listMembersType)
        {
            SearchResult<ProfileMember> searchResultProfileMember = profileAPI.searchProfileMembers(memberType,searchOptionsBuilder.done());
            for (ProfileMember profileMember : searchResultProfileMember.getResult())
            {
              SearchOptionsBuilder searchProfileMember = new SearchOptionsBuilder(0,100);
              // membership : contains a Groupid and a roleid
              
              if (profileMember.getRoleId()>0 )
              {
                searchProfileMember.filter(UserSearchDescriptor.ROLE_ID,profileMember.getRoleId() );
              }
              if (profileMember.getGroupId()>0 )
              {
                searchProfileMember.filter(UserSearchDescriptor.GROUP_ID,profileMember.getGroupId() );
              }
              if (profileMember.getUserId()>0 )
              {
                searchProfileMember.filter(UserSearchDescriptor.ID,profileMember.getUserId() );
              }
              
              searchProfileMember.filter(UserSearchDescriptor.ENABLED, Boolean.TRUE);
              
              SearchResult<User> listUsers = identityAPI.searchUsers( searchProfileMember.done());
              for (User user : listUsers.getResult())
                  userIdToSendEmail.add( user.getId());

            }
        } // end for collect user
        
        
        //------------------------ now run each users
        int maxTaxInEmail = input.getInputLongParameter(cstParamMaxTaskInEmail).intValue();
        String bonitaHost = input.getInputStringParameter(cstParamBonitaHost);
        long bonitaPort = input.getInputLongParameter(cstParamBonitaPort);
        int numberOfEmailsSent=0;
        String messageOperation="";
        StringBuffer totalMessageOperation=new StringBuffer();
        for (Long userId : userIdToSendEmail)
        {
            // do this user has a task ?
            List<HumanTaskInstance> listAssignedHumanTask = processAPI.getAssignedHumanTaskInstances(userId, 0, maxTaxInEmail, ActivityInstanceCriterion.EXPECTED_END_DATE_DESC);
            
            List<HumanTaskInstance> listPendingHumanTask = processAPI.getPendingHumanTaskInstances(userId, 0, maxTaxInEmail, ActivityInstanceCriterion.EXPECTED_END_DATE_DESC);
            
            if (listAssignedHumanTask.size() + listPendingHumanTask.size()==0)
                continue;
            
                // send an email to this users !
                String contentEmail = input.getInputStringParameter(cstParamEmailText);
                String contentAssignedTask  = getHtmlListTasks( listAssignedHumanTask, bonitaHost, bonitaPort, processAPI);
                String contentPendingTask   = getHtmlListTasks( listPendingHumanTask, bonitaHost, bonitaPort, processAPI);
                // search and replace the different place holder
                if (contentEmail.indexOf("{{assignedtasks}}")!=-1)
                    contentEmail = contentEmail.replaceAll("{{assignedtasks}}", contentAssignedTask);
                else
                    contentEmail+="<p>"+contentAssignedTask;
                
                if (contentEmail.indexOf("{{pendingtasks}}")!=-1)
                    contentEmail = contentEmail.replaceAll("{{pendingtasks}}", contentPendingTask);
                else
                    contentEmail+="<p>"+contentPendingTask;
                
                ContactData contactData = identityAPI.getUserContactData(userId, false);
                // send email
                if (contactData.getEmail() == null)
                    messageOperation+="No email";
                else
                {
                    
                //ok, we have all we need
                List<BEvent> listEvents= sendEmail(contactData.getEmail(), contentEmail, input);
                if (BEventFactory.isError(listEvents))
                    messageOperation+="Error emails "+BEventFactory.getHtml(listEvents);
                else
                    numberOfEmailsSent++;
                }
            
            
                if (messageOperation.length()>0)
                {
                    User user = identityAPI.getUser( userId );
                    totalMessageOperation.append("User["+user.getUserName()+"] : "+messageOperation);
                }
                
            
        }
        if (totalMessageOperation.toString().length()==0)
        {
            // No Error !
            if (numberOfEmailsSent==0)
                plugTourOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
            else
                plugTourOutput.executionStatus = ExecutionStatus.SUCCESS;
            plugTourOutput.addEvent( new BEvent( EVENT_OPERATION_SUCCESS, "Users checks: "+userIdToSendEmail.size()+", Emails sent: "+numberOfEmailsSent));
        }
        else
        {
            plugTourOutput.executionStatus = ExecutionStatus.ERROR;
            String totalMessageOperationSt = totalMessageOperation.toString();
            if (totalMessageOperationSt.length()>1000)
                totalMessageOperationSt = totalMessageOperationSt.substring(0, 1000)+"...";
            plugTourOutput.addEvent( new BEvent( EVENT_OPERATION_ERROR, "Users checks: "+userIdToSendEmail.size()+", Emails sent: "+numberOfEmailsSent+",Error:"+totalMessageOperationSt));
        }
        } catch (Exception e)
        {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();

            logger.severe("MilkEmailUsersTasks exception "+e+" at "+exceptionDetails);
            plugTourOutput.addEvent( new BEvent( EVENT_OPERATION_ERROR, e, ""));
        }
        return plugTourOutput;
    }
    
        
        /**
         * Create the HTML list
         */
        static SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm");
   private String getHtmlListTasks( List<HumanTaskInstance> listHumanTasks, String bonitaHost, Long bonitaPort, ProcessAPI processAPI)
   {
       StringBuffer content= new StringBuffer();
       content.append("<table><tr><th>Task</th><th>Due date</th></tr>");
       for (HumanTaskInstance task : listHumanTasks)
       {
           content.append("<tr>");
       
           content.append( "<td><a href=\"http://"+bonitaHost+":"+bonitaPort+"/bonita/portal/form/taskInstance/"+task.getId()+">");
           if (task.getDisplayName()==null)
               content.append(task.getName());
               else
                   content.append(task.getDisplayName());
           content.append("</a></td>");
           
           content.append("<td>");
           if (task.getExpectedEndDate()!= null)
           {
               content.append(sdf.format(task.getExpectedEndDate() )); 
           }
           content.append("</td>");
           content.append("</tr>");
       }
       return content.toString();
       
   }
   
   /**
    * send the email
    * @param emailTo
    * @param textMessage
    * @param input
    * @return
    */
    private List<BEvent> sendEmail(String emailTo, String textMessage, PlugTourInput input)
    {

        List<BEvent>  listEvents = new ArrayList<BEvent>();
        
        Properties properties = System.getProperties();
        properties.put("mail.smtp.auth", input.getInputBooleanParameter(cstParamSmtpUseSSL));
        properties.put("mail.smtp.starttls.enable", input.getInputBooleanParameter(cstParamSmtpStartTLS) );
        properties.put("mail.smtp.host", input.getInputStringParameter(cstParamEmailSmtpHost));
        properties.put("mail.smtp.port",  input.getInputLongParameter(cstParamEmailSmtpPort));
        properties.put("mail.smtp.ssl.trust",input.getInputStringParameter(cstParamSmtpSSLTrust));
        
        Session session;
        final String userName = input.getInputStringParameter(cstParamSmtpUserName);
        final String userPassword = input.getInputStringParameter(cstParamSmtpUserPassword);
        if (userName==null || userName.length()==0)
        {
            session = Session.getDefaultInstance(properties);
            
        }
        else
        {
            session = Session.getInstance(properties, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(userName, userPassword);
            }
        });
        }
       
            // Sender's email ID needs to be mentioned
            String emailFrom = input.getInputStringParameter(cstParamEmailFrom);


            // Get the default Session object.
            try {
               // Create a default MimeMessage object.
               MimeMessage message = new MimeMessage(session);

               // Set From: header field of the header.
               message.setFrom(new InternetAddress(emailFrom));

               // Set To: header field of the header.
               message.addRecipient(Message.RecipientType.TO, new InternetAddress(emailTo));

               // Set Subject: header field
               message.setSubject(input.getInputStringParameter(cstParamEmailSubject));

               // Now set the actual message
               message.setText(textMessage);

               // Send message
               Transport.send(message);
               
            } catch (MessagingException e) {
                listEvents.add( new BEvent( EVENT_SEND_EMAIL, e,"Mail to ["+emailTo+"] "));
            }
         
    return listEvents;
    }
}
