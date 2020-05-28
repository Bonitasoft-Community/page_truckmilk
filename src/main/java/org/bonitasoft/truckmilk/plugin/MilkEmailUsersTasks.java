package org.bonitasoft.truckmilk.plugin;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.bonitasoft.engine.api.IdentityAPI;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.api.ProfileAPI;
import org.bonitasoft.engine.bpm.flownode.ActivityInstanceCriterion;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstance;
import org.bonitasoft.engine.identity.ContactData;
import org.bonitasoft.engine.identity.User;
import org.bonitasoft.engine.identity.UserSearchDescriptor;
import org.bonitasoft.engine.profile.Profile;
import org.bonitasoft.engine.profile.ProfileMember;
import org.bonitasoft.engine.profile.ProfileMemberSearchDescriptor;
import org.bonitasoft.engine.profile.ProfileSearchDescriptor;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.job.MilkJobExecution;
import org.bonitasoft.truckmilk.toolbox.MilkLog;
import org.bonitasoft.truckmilk.toolbox.SendMail;
import org.bonitasoft.truckmilk.toolbox.SendMailEnvironment;
import org.bonitasoft.truckmilk.toolbox.SendMailParameters;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobContext;

public class MilkEmailUsersTasks extends MilkPlugIn {

    public static MilkLog logger = MilkLog.getLogger(MilkEmailUsersTasks.class.getName());

    private static BEvent EVENT_SEND_EMAIL = new BEvent(MilkEmailUsersTasks.class.getName(), 1, Level.APPLICATIONERROR,
            "Mail send failed", "Email can't be send", "No email send", "Check the error");

    private static BEvent EVENT_OPERATION_SUCCESS = new BEvent(MilkEmailUsersTasks.class.getName(), 2, Level.SUCCESS,
            "Mails was sent if needed", "Mails was sent");

    private static BEvent EVENT_OPERATION_ERROR = new BEvent(MilkEmailUsersTasks.class.getName(), 3, Level.APPLICATIONERROR,
            "Error during sending", "Errors arrived : no email in users, smtp server down", "Some emails failed", "Check errors");

    private static BEvent EVENT_PROFILES_DOES_NOT_EXIST = new BEvent(MilkEmailUsersTasks.class.getName(), 4, Level.APPLICATIONERROR,
            "Profiles does not exist", "The profiles does not exist", "Operation failed", "Give an existing profile name");

    private static PlugInParameter cstParamProfilesUser = PlugInParameter.createInstance("profilesuser", "Profiles User", TypeParameter.ARRAY, null, "Give a list of Profiles User. All users referenced in theses profiles are inspected, to detet if they have tasks to execute");

    private static PlugInParameter cstParamEmailFrom = PlugInParameter.createInstance("emailfrom", "Email From", TypeParameter.STRING, "bonitasoftreminder@myserver.com", "The 'mail from' attribute. Give a name, then the user can confirms this transmitter in its anti spam");
    private static PlugInParameter cstParamEmailSubject = PlugInParameter.createInstance("emailsubject", "Email Subject", TypeParameter.STRING, "You have some pending tasks", "Subject on the email");
    private static PlugInParameter cstParamEmailText = PlugInParameter.createInstance("emailtext", "Email Text", TypeParameter.TEXT, "Your assigned task <br>{{assignedtasks}}<p>Your pending tasks:<br>{{pendingtasks}}", "Content of the email. Use the place holder {{assignedtasks}} and {{pendingtasks}}");

    private static PlugInParameter cstParamMaxTaskInEmail = PlugInParameter.createInstance("maxtasksinemail", "Maximum tasks in the mail", TypeParameter.LONG, 30L, "Number of task to display in the email");

    private static PlugInParameter cstParamBonitaHost = PlugInParameter.createInstance("bonitahost", "Bonita Host", TypeParameter.STRING, "http://localhost", "In the Email, the HTTP link will use this information");
    private static PlugInParameter cstParamBonitaPort = PlugInParameter.createInstance("bonitaport", "Bonita Port", TypeParameter.LONG, 8080L, "In the Email, the HTTP link will use this information");

    public MilkEmailUsersTasks() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * check the plugin environment : for the milkEmailUsersTasks, we require to be able to send an email
     */
    public List<BEvent> checkPluginEnvironment(MilkJobExecution milkJobExecution) {
        return SendMailEnvironment.checkEnvironment(milkJobExecution, this);
    };

    /**
     * check the Job's environment
     */
    public List<BEvent> checkJobEnvironment(MilkJobExecution milkJobExecution) {
        List<BEvent> listEvents = new ArrayList<>();
        return listEvents;
    };

    @Override
    public MilkPlugInDescription getDefinitionDescription(MilkJobContext milkJobContext) {
        MilkPlugInDescription plugInDescription = new MilkPlugInDescription();
        plugInDescription.setName( "EmailUsersTasks");
        plugInDescription.setLabel( "Emails users tasks");
        plugInDescription.setExplanation( "For all users part of the given profile(s), an email is send with a link on all pending tasks");
        plugInDescription.setCategory( CATEGORY.TASKS);
        plugInDescription.setStopJob( JOBSTOPPER.BOTH );
        plugInDescription.addParameter(cstParamProfilesUser);

        try {
            SendMailParameters.addPlugInParameter(SendMailParameters.MAIL_DIRECTION.SENDONLY, plugInDescription);
        } catch (Error er) {
            // do nothing here, the error will show up again in the check Environment
            // Cause : the Email Jar file is not installed, then java.lang.NoClassDefFoundError: javax/mail/Address
        }
        plugInDescription.addParameter(cstParamBonitaHost);
        plugInDescription.addParameter(cstParamBonitaPort);
        plugInDescription.addParameter(cstParamEmailFrom);
        plugInDescription.addParameter(cstParamEmailSubject);
        plugInDescription.addParameter(cstParamEmailText);
        plugInDescription.addParameter(cstParamMaxTaskInEmail);

        return plugInDescription;
    }

    @SuppressWarnings("unchecked")
    @Override
    public MilkJobOutput execute(MilkJobExecution jobExecution) {
        MilkJobOutput plugTourOutput = jobExecution.getMilkJobOutput();

        // read profiles
        List<String> listProfilesName = (List<String>) jobExecution.getInputListParameter(cstParamProfilesUser);
        if (listProfilesName.isEmpty() ) {
            plugTourOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
            return plugTourOutput;
        }
        jobExecution.setAvancementTotalStep(300);

        try {

            // first 100 points here
            int baseAdvancement = 0;
            ProfileAPI profileAPI = jobExecution.getApiAccessor().getProfileAPI();
            ProcessAPI processAPI = jobExecution.getApiAccessor().getProcessAPI();
            IdentityAPI identityAPI = jobExecution.getApiAccessor().getIdentityAPI();
            // collect users in profiles
            List<Long> userIdToSendEmail = new ArrayList<>();
            List<Long> listProfileId = new ArrayList<>();
            for (int i = 0; i < listProfilesName.size(); i++) {
                jobExecution.setAvancementTotalStep(baseAdvancement + (100 * i) / listProfilesName.size());
                if (jobExecution.pleaseStop())
                    break;

                SearchOptionsBuilder searchOptionsBuilder = new SearchOptionsBuilder(0, 10000);

                searchOptionsBuilder.filter(ProfileSearchDescriptor.NAME, listProfilesName.get(i));
                SearchResult<Profile> searchResultProfiles = profileAPI.searchProfiles(searchOptionsBuilder.done());
                if (searchResultProfiles.getCount() == 0) {
                    plugTourOutput.addEvent(new BEvent(EVENT_PROFILES_DOES_NOT_EXIST, "profile[" + listProfilesName.get(i)));
                } else {
                    for (Profile profile : searchResultProfiles.getResult()) {
                        listProfileId.add(profile.getId());
                    }
                }
            }
            baseAdvancement = 100;
            jobExecution.setAvancementTotalStep(baseAdvancement);

            if (listProfileId.isEmpty()) {
                plugTourOutput.executionStatus = ExecutionStatus.WARNING;
                return plugTourOutput;
            }
            //---------------- then search each profiles member
            // second step
            SearchOptionsBuilder searchOptionsBuilder = new SearchOptionsBuilder(0, 10000);
            for (int i = 0; i < listProfileId.size(); i++) {
                jobExecution.setAvancementTotalStep(baseAdvancement + (100 * i) / listProfileId.size());
                if (jobExecution.pleaseStop())
                    break;

                if (i > 0)
                    searchOptionsBuilder.or();
                searchOptionsBuilder.filter(ProfileMemberSearchDescriptor.PROFILE_ID, listProfileId.get(i));
            }

            String[] listMembersType = new String[] { "user", "role", "group", "roleAndGroup" };
            for (String memberType : listMembersType) {
                SearchResult<ProfileMember> searchResultProfileMember = profileAPI.searchProfileMembers(memberType, searchOptionsBuilder.done());
                for (ProfileMember profileMember : searchResultProfileMember.getResult()) {
                    SearchOptionsBuilder searchProfileMember = new SearchOptionsBuilder(0, 100);
                    // membership : contains a Groupid and a roleid

                    if (profileMember.getRoleId() > 0) {
                        searchProfileMember.filter(UserSearchDescriptor.ROLE_ID, profileMember.getRoleId());
                    }
                    if (profileMember.getGroupId() > 0) {
                        searchProfileMember.filter(UserSearchDescriptor.GROUP_ID, profileMember.getGroupId());
                    }
                    if (profileMember.getUserId() > 0) {
                        searchProfileMember.filter(UserSearchDescriptor.ID, profileMember.getUserId());
                    }

                    searchProfileMember.filter(UserSearchDescriptor.ENABLED, Boolean.TRUE);

                    SearchResult<User> listUsers = identityAPI.searchUsers(searchProfileMember.done());
                    for (User user : listUsers.getResult())
                        userIdToSendEmail.add(user.getId());

                }
            } // end for collect user

            //------------------------ now run each users
            // 
            baseAdvancement = 200;
            jobExecution.setAvancementTotalStep(baseAdvancement);

            int maxTaxInEmail = jobExecution.getInputLongParameter(cstParamMaxTaskInEmail).intValue();
            String bonitaHost = jobExecution.getInputStringParameter(cstParamBonitaHost);
            long bonitaPort = jobExecution.getInputLongParameter(cstParamBonitaPort);
            int numberOfEmailsSent = 0;
            StringBuilder messageOperation = new StringBuilder();
            StringBuilder totalMessageOperation = new StringBuilder();
            for (int i = 0; i < userIdToSendEmail.size(); i++) {

                jobExecution.setAvancementTotalStep(baseAdvancement + (100L * i) / userIdToSendEmail.size());
                if (jobExecution.pleaseStop())
                    break;

                Long userId = userIdToSendEmail.get(i);
                // do this user has a task ?
                List<HumanTaskInstance> listAssignedHumanTask = processAPI.getAssignedHumanTaskInstances(userId, 0, maxTaxInEmail, ActivityInstanceCriterion.EXPECTED_END_DATE_DESC);

                List<HumanTaskInstance> listPendingHumanTask = processAPI.getPendingHumanTaskInstances(userId, 0, maxTaxInEmail, ActivityInstanceCriterion.EXPECTED_END_DATE_DESC);

                if (listAssignedHumanTask.size() + listPendingHumanTask.size() == 0)
                    continue;

                // send an email to this users !
                String contentEmail = jobExecution.getInputStringParameter(cstParamEmailText);
                String contentAssignedTask = getHtmlListTasks(listAssignedHumanTask, bonitaHost, bonitaPort, processAPI);
                String contentPendingTask = getHtmlListTasks(listPendingHumanTask, bonitaHost, bonitaPort, processAPI);
                // search and replace the different place holder
                if (contentEmail.indexOf("{{assignedtasks}}") != -1)
                    contentEmail = contentEmail.replaceAll("\\{\\{assignedtasks\\}\\}", contentAssignedTask);
                else
                    contentEmail += "<p>" + contentAssignedTask;

                if (contentEmail.indexOf("{{pendingtasks}}") != -1)
                    contentEmail = contentEmail.replaceAll("\\{\\{pendingtasks\\}\\}", contentPendingTask);
                else
                    contentEmail += "<p>" + contentPendingTask;

                ContactData contactData = identityAPI.getUserContactData(userId, false);
                // send email
                if (contactData.getEmail() == null)
                    messageOperation.append( "No email" );
                else {

                    //ok, we have all we need
                    List<BEvent> listEvents = sendEmail(contactData.getEmail(), contentEmail, jobExecution);
                    if (BEventFactory.isError(listEvents))
                        messageOperation.append( "Error emails " + BEventFactory.getHtml(listEvents));
                    else
                        numberOfEmailsSent++;
                }

                if (messageOperation.length() > 0) {
                    User user = identityAPI.getUser(userId);
                    totalMessageOperation.append("User[" + user.getUserName() + "] : " + messageOperation.toString());
                }

            }
            if (totalMessageOperation.toString().length() == 0) {
                // No Error !
                if (numberOfEmailsSent == 0)
                    plugTourOutput.executionStatus = ExecutionStatus.SUCCESSNOTHING;
                else
                    plugTourOutput.executionStatus = ExecutionStatus.SUCCESS;
                plugTourOutput.addEvent(new BEvent(EVENT_OPERATION_SUCCESS, "Users checks: " + userIdToSendEmail.size() + ", Emails sent: " + numberOfEmailsSent));
            } else {
                plugTourOutput.executionStatus = ExecutionStatus.ERROR;
                String totalMessageOperationSt = totalMessageOperation.toString();
                if (totalMessageOperationSt.length() > 1000)
                    totalMessageOperationSt = totalMessageOperationSt.substring(0, 1000) + "...";
                plugTourOutput.addEvent(new BEvent(EVENT_OPERATION_ERROR, "Users checks: " + userIdToSendEmail.size() + ", Emails sent: " + numberOfEmailsSent + ",Error:" + totalMessageOperationSt));
            }
            if (plugTourOutput.executionStatus == ExecutionStatus.SUCCESS && jobExecution.pleaseStop())
                plugTourOutput.executionStatus = ExecutionStatus.SUCCESSPARTIAL;

        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();

            logger.severe("MilkEmailUsersTasks exception " + e + " at " + exceptionDetails);
            plugTourOutput.addEvent(new BEvent(EVENT_OPERATION_ERROR, e, ""));
            plugTourOutput.executionStatus = ExecutionStatus.ERROR;
        }
        return plugTourOutput;
    }

    /**
     * Create the HTML list
     */
    static SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm");

    private String getHtmlListTasks(List<HumanTaskInstance> listHumanTasks, String bonitaHost, Long bonitaPort, ProcessAPI processAPI) {
        StringBuilder content = new StringBuilder();
        content.append("<table style=\"border-collapse: collapse;border-spacing: 0;padding: 8px;line-height: 1.42857143;vertical-align: top;border-top: 1px solid #ecf0f1;background-color: #d9edf7\">");
        content.append("<tr  style=\"background-color: #dd0033;color: white;font-size: small;border-bottom-color: #666;border-bottom: 2px solid #ecf0f1;font-family: 'Lato', 'Helvetica Neue', Helvetica, Arial, sans-serif;\">");
        content.append("<th>Case Id</th><th>Task</th><th>Due date</th></tr>");
        for (HumanTaskInstance task : listHumanTasks) {
            content.append("<tr style=\"color: #323232;box-sizing: border-box;\">");

            content.append("<td>" + task.getRootContainerId() + "</td>");

            content.append("<td><a href=\"http://" + bonitaHost + ":" + bonitaPort + "/bonita/portal/form/taskInstance/" + task.getId() + "\">");
            if (task.getDisplayName() == null)
                content.append(task.getName());
            else
                content.append(task.getDisplayName());
            content.append("</a></td>");

            content.append("<td>");
            if (task.getExpectedEndDate() != null) {
                content.append(sdf.format(task.getExpectedEndDate()));
            }
            content.append("</td>");
            content.append("</tr>");
        }
        return content.toString();

    }

    /**
     * send the email
     * 
     * @param emailTo
     * @param textMessage
     * @param input
     * @return
     */
    private List<BEvent> sendEmail(String emailTo, String textMessage, MilkJobExecution input) {

        List<BEvent> listEvents = new ArrayList<>();
        // Sender's email ID needs to be mentioned
        String emailFrom = input.getInputStringParameter(cstParamEmailFrom);
        SendMail sendMail = new SendMail(input);

        listEvents.addAll(sendMail.sendMail(emailTo, emailFrom, input.getInputStringParameter(cstParamEmailSubject), textMessage));

        return listEvents;
    }
}
