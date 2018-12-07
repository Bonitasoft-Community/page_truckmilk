package org.bonitasoft.truckmilk.toolbox;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugInDescription;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugTourInput;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.TypeParameter;

/**
 * send an email
 */
public class SendMail {

  private static BEvent eventMailError = new BEvent(SendMail.class.getName(), 1,
      Level.APPLICATIONERROR,
      "Mail error", "Error during sending an email",
      "Email was not sent",
      "Check the error");

   public String smtpHost;
  public Integer smtpPort;
  public String smtpUser;
  public String smtpPassword;

  
  private static PlugInParameter cstParamSmtpParameters = PlugInParameter.createInstance("SmtpParameters", TypeParameter.STRING, "localhost:25", "Smtp parameters: host:port:username:password");
  public static PlugInParameter cstParamTestSendMail = PlugInParameter.createInstanceButton("SmtpTest",  Arrays.asList("To email"), Arrays.asList("youremail@company.com"), "Give a Email to test the SMTP Connection", "Give parameters and click on the button to verify that the email can be send");

  public static enum MAIL_DIRECTION {
    SENDONLY, READONLY, SENDREAD
  };

  public static void addPlugInParameter(MAIL_DIRECTION mailDirection, PlugInDescription plugInDescription) {
    // in all case, the Stmp Parameters is required
    plugInDescription.addParameter(cstParamSmtpParameters);
    // Nota : the code of the TestButton must be included in the MilkPlugIn whoi 
    plugInDescription.addParameter(cstParamTestSendMail);

    // according what we want, then add different parameters
  }

 

  /**
   * parameters in a string : host:port:username:password
   * Default Constructor.
   * 
   * @param parameters
   */
  public SendMail(PlugTourInput input) {

    if (input.getInputStringParameter(cstParamSmtpParameters)!=null)
    {
      StringTokenizer st = new StringTokenizer(input.getInputStringParameter(cstParamSmtpParameters), ":");
  
      this.smtpHost = st.hasMoreTokens() ? st.nextToken() : "";
      this.smtpPort = st.hasMoreTokens() ? Integer.valueOf(st.nextToken()) : 0;
      this.smtpUser = st.hasMoreTokens() ? st.nextToken() : "";
      this.smtpPassword = st.hasMoreTokens() ? st.nextToken() : "";
    }    
  }

  public SendMail(String smtpHost, Integer smtpPort, String smtpUserName, String smtpPassword) {
    this.smtpHost = smtpHost;
    this.smtpPort = smtpPort;
    this.smtpUser = smtpUserName;
    this.smtpPassword = smtpPassword;
  }

  public List<BEvent> sendMail(String toMail, String fromMail, String subject, String contentHtml) {

    List<BEvent> listEvents = new ArrayList<BEvent>();

    Properties props = new Properties();
    props.put("mail.smtp.host", smtpHost);
    props.put("mail.smtp.auth", smtpUser != null ? "true" : "false");
    Session session = Session.getDefaultInstance(props);
    session.setDebug(true);
    MimeMessage message = new MimeMessage(session);
    try {
      message.setFrom(new InternetAddress(fromMail));
      message.addRecipient(Message.RecipientType.TO, new InternetAddress(toMail));
      message.setSubject(subject);
      message.setText(contentHtml, "UTF-8", "html");
      // message.saveChanges();
      Transport transport = session.getTransport("smtp");
      transport.connect(smtpHost, smtpPort, smtpUser, smtpPassword);
      transport.sendMessage(message, message.getAllRecipients());
      transport.close();
    } catch (Exception e) {
      listEvents.add(new BEvent(eventMailError, "Send Mail to [" + toMail + "] " + e.getMessage()));
    }
    return listEvents;
  }

  // see SendMailEvironment.checkEnvironment
  // public static List<BEvent> checkEnvironment(long tenantId) {

  public List<BEvent> verifyEmailDeployed(boolean testTheConnection) {
    List<BEvent> listEvents = new ArrayList<BEvent>();

    try {
      Properties props = new Properties();
      // net set a null in a properties...
      props.put("mail.smtp.host", smtpHost==null ? "localhost": smtpHost);
      Session session = Session.getDefaultInstance(props);
      session.setDebug(true);
      // message.saveChanges();
      Transport transport = session.getTransport("smtp");
      if (testTheConnection)
        transport.connect(smtpHost, smtpPort, smtpUser, smtpPassword);
    } catch (Exception e) {
      listEvents.add(SendMailEnvironment.EVENT_MAIL_NOTDEPLOYED);
    }
   catch (Error er) {
    listEvents.add(SendMailEnvironment.EVENT_MAIL_NOTDEPLOYED);
   }  
    return listEvents;
  }
  
  public List<BEvent> testEmail( Map<String, Object> argsParameters ) {
    String toEmail = (String) argsParameters.get("To email");

    return sendMail(toEmail, "from@bonitasoft.com", "Test the connection", "Truck milk page test");
  }

}
