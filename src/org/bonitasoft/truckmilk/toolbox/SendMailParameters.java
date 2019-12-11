package org.bonitasoft.truckmilk.toolbox;

import java.util.Arrays;
import java.util.Properties;
import java.util.StringTokenizer;

import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.job.MilkJobExecution;

/**
 * Use a different class, to let the parameter be added even if an issue arrive because the email class is not installed
 */
public class SendMailParameters {

    public String smtpHost;
    public Integer smtpPort;
    public String smtpUser;
    public String smtpPassword;

    /*
     * private static PlugInParameter cstParamSmtpUseSSL = PlugInParameter.createInstance("smtpusessl", "SMTP use SSL", TypeParameter.BOOLEAN, Boolean.FALSE,
     * "If true, the SMTP need a SSL authentication");
     * private static PlugInParameter cstParamSmtpStartTLS = PlugInParameter.createInstance("smtpstarttls", "SMTP start TLS", TypeParameter.BOOLEAN,
     * Boolean.FALSE, "If true, the SMTP need a TLS");
     * private static PlugInParameter cstParamSmtpSSLTrust = PlugInParameter.createInstance("smtpssltrust", "SMTP Trust SSL", TypeParameter.BOOLEAN, "",
     * "If true, the STMP trust the SSL mechanism");
     */
    /*
     * private static PlugInParameter cstParamEmailSmtpHost = PlugInParameter.createInstance("smtphost", "SMTP Host",TypeParameter.STRING, "smtp.gmail.com",
     * "Smtp host name");
     * private static PlugInParameter cstParamEmailSmtpPort = PlugInParameter.createInstance("smtpport", "SMTP Port", TypeParameter.LONG, 465L, "Smtp port");
     * private static PlugInParameter cstParamSmtpUserName = PlugInParameter.createInstance("smtpusername", "SMTP User Name", TypeParameter.STRING, null,
     * "If you server require an authentication, give the login/password");
     * private static PlugInParameter cstParamSmtpUserPassword = PlugInParameter.createInstance("smtpuserpassword", "SMTP User Password", TypeParameter.STRING,
     * null, "If you server require an authentication, give the login/password");
     */

    /** replace the paramter SmtpHost, port, username, userpassord */
    private static PlugInParameter cstParamSmtpParameters = PlugInParameter.createInstance("SmtpParameters", "SMTP Parameters", TypeParameter.STRING, "localhost:25", "Smtp parameters: host:port:username:password");
    public static PlugInParameter cstParamTestSendMail = PlugInParameter.createInstanceButton("SmtpTest", "SMTP Test", Arrays.asList("To email"), Arrays.asList("youremail@company.com"), "Give a Email to test the SMTP Connection", "Give parameters and click on the button to verify that the email can be send");

    
    private static PlugInParameter cstParamSeparatorSmtp = PlugInParameter.createInstance("Smtp", "SMTP Parameters", TypeParameter.SEPARATOR, "", "To send an email, SMTP parameters is required to connect a SMTP server");
    
    public static enum MAIL_DIRECTION {
        SENDONLY, READONLY, SENDREAD
    };

    public SendMailParameters(MilkJobExecution input) {

        if (input.getInputStringParameter(cstParamSmtpParameters) != null) {
            StringTokenizer st = new StringTokenizer(input.getInputStringParameter(cstParamSmtpParameters), ":");

            this.smtpHost = st.hasMoreTokens() ? st.nextToken() : "";
            this.smtpPort = st.hasMoreTokens() ? Integer.valueOf(st.nextToken()) : 0;
            this.smtpUser = st.hasMoreTokens() ? st.nextToken() : "";
            this.smtpPassword = st.hasMoreTokens() ? st.nextToken() : "";
        }
    }

    public SendMailParameters(String smtpHost, Integer smtpPort, String smtpUserName, String smtpPassword) {
        this.smtpHost = smtpHost;
        this.smtpPort = smtpPort;
        this.smtpUser = smtpUserName;
        this.smtpPassword = smtpPassword;
    }

    /**
     * fullfill parameters
     * 
     * @param props
     */
    public void fullFillParameters(Properties props) {

        props.put("mail.smtp.host", smtpHost);
        props.put("mail.smtp.auth", smtpUser != null ? "true" : "false");
    }

    public static void addPlugInParameter(MAIL_DIRECTION mailDirection, PlugInDescription plugInDescription) {
        // in all case, the Stmp Parameters is required
        plugInDescription.addParameter(cstParamSeparatorSmtp);
        plugInDescription.addParameter(cstParamSmtpParameters);

        // according what we want, then add different parameters
    }

    public static void addPlugInAnalysis(MAIL_DIRECTION mailDirection, PlugInDescription plugInDescription) {

        // Nota : the code of the TestButton must be included in the MilkPlugIn 
        plugInDescription.addAnalysisParameter(cstParamTestSendMail);

        // according what we want, then add different parameters
    }
}
