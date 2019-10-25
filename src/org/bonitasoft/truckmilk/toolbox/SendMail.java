package org.bonitasoft.truckmilk.toolbox;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.job.MilkJobExecution;

/**
 * send an email
 */
public class SendMail {

    private static BEvent EVENT_MAIL_ERROR = new BEvent(SendMail.class.getName(), 1,
            Level.APPLICATIONERROR,
            "Mail error", "Error during sending an email",
            "Email was not sent",
            "Check the error");

    SendMailParameters sendMailParameters;
  /**
     * parameters in a string : host:port:username:password
     * Default Constructor.
     * 
     * @param parameters
     */
    public SendMail(MilkJobExecution input) {
        sendMailParameters = new SendMailParameters(input);
    }

    public SendMail(String smtpHost, Integer smtpPort, String smtpUserName, String smtpPassword) {
        sendMailParameters = new SendMailParameters(smtpHost, smtpPort, smtpUserName, smtpPassword);
    }

    public List<BEvent> sendMail(String toMail, String fromMail, String subject, String contentHtml) {

        List<BEvent> listEvents = new ArrayList<BEvent>();

        Properties props = new Properties();
        sendMailParameters.fullFillParameters( props);
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
            transport.connect(sendMailParameters.smtpHost, sendMailParameters.smtpPort, sendMailParameters.smtpUser, sendMailParameters.smtpPassword);
            transport.sendMessage(message, message.getAllRecipients());
            transport.close();
        } catch (Exception e) {
            listEvents.add(new BEvent(EVENT_MAIL_ERROR, "Send Mail to [" + toMail + "] " + e.getMessage()));
        }
        return listEvents;
    }

    // see SendMailEvironment.checkEnvironment
    // public static List<BEvent> checkEnvironment(long tenantId) {

    public List<BEvent> verifyEmailDeployed(boolean testTheConnection) {
        List<BEvent> listEvents = new ArrayList<BEvent>();

        try {
            Properties props = new Properties();
            sendMailParameters.fullFillParameters( props);
            // net set a null in a properties...

            Session session = Session.getDefaultInstance(props);
            session.setDebug(true);
            // message.saveChanges();
            Transport transport = session.getTransport("smtp");
            if (testTheConnection)
                transport.connect(sendMailParameters.smtpHost, sendMailParameters.smtpPort, sendMailParameters.smtpUser, sendMailParameters.smtpPassword);
        } catch (Exception e) {
            listEvents.add(SendMailEnvironment.EVENT_MAIL_NOTDEPLOYED);
        } catch (Error er) {
            listEvents.add(SendMailEnvironment.EVENT_MAIL_NOTDEPLOYED);
        }
        return listEvents;
    }

    public List<BEvent> testEmail(Map<String, Object> argsParameters) {
        String toEmail = (String) argsParameters.get("To email");

        return sendMail(toEmail, "from@bonitasoft.com", "Test the connection", "Truck milk page test");
    }

}
