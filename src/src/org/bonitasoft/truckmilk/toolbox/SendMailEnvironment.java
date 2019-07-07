package org.bonitasoft.truckmilk.toolbox;

import java.util.ArrayList;
import java.util.List;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn;

/**
 * the main error when we want to verify that the email is ready is the fact the activation Mail class is correclty deployed.
 * But to verify that, the verification must be in a class who don't use at all any reference to the mail class, so in a different class.
 * @author Firstname Lastname
 *
 */
public class SendMailEnvironment {

  // this event must be here, and not in the SendEmailClass : any try to load the sendEma
  public static BEvent EVENT_MAIL_NOTDEPLOYED = new BEvent(SendMailEnvironment.class.getName(), 2,
      Level.ERROR,
      "Mail Not deployed", "Mail librairy are not deployed",
      "Emails can't be send",
      "Copy activation-1.1.jar and mail-1.5.0-b01.jar in the LIB folder (example, <TOMCAT>/lib folder)");

  /**
   * verify that the environment is ready
   * 
   * @param tenantId
   * @return
   */
  public static List<BEvent> checkEnvironment(long tenantId, MilkPlugIn plugInRequester) {
    List<BEvent> listEvents = new ArrayList<BEvent>();
    // verify that 
    // check if the mail is deployed
    try {
      SendMail sendMail = new SendMail(null, null, null, null);
      listEvents.addAll(sendMail.verifyEmailDeployed(false));
    }
    catch (Error er) {
      listEvents.add( new BEvent(EVENT_MAIL_NOTDEPLOYED, "PlugIn:"+plugInRequester.getDescription().displayName));
     }  
    return listEvents;

  };
}
