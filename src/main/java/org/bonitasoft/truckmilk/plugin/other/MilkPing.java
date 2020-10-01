package org.bonitasoft.truckmilk.plugin.other;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.IdentityAPI;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.identity.User;
import org.bonitasoft.engine.identity.UserSearchDescriptor;
import org.bonitasoft.engine.search.Order;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.engine.MilkJobOutput;
import org.bonitasoft.truckmilk.engine.MilkJobOutput.Chronometer;
import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.CATEGORY;
import org.bonitasoft.truckmilk.engine.MilkPlugInDescription.JOBSTOPPER;
import org.bonitasoft.truckmilk.job.MilkJob.ExecutionStatus;
import org.bonitasoft.truckmilk.job.MilkJobContext;
/* ******************************************************************************** */
/*                                                                                  */
/* Ping */
/*                                                                                  */
/* this class may be use as a skeleton for a new plug in */
/* Attention, reference any new plug in in MilPlugInFactory.collectListPlugIn() */
/*                                                                                  */
/* ******************************************************************************** */
import org.bonitasoft.truckmilk.job.MilkJobExecution;

/**
 * Welcome to the PING job !
 * This example is here to explain how to develop your own job. Here the step
 * 1. Create a class, and extend MilkPlugIn
 * This abstract class is the definition of a job. You can use any package under org.bonitasoft.truckmilk.plugin
 * The rule is to have a sub process per CATEGORY. The category explain on which component the job works : it may be tasks, cases, monitor...
 * You can set the type to TYPE_PLUGIN.EMBEDED : that's mean this plug in is delivered with the Truckmilk page.
 * 2. Register your plug in in MilkPlugInFactory.collectListPlugIn()
 * Then, the factory knows your plugin and can propose it to the users
 * 3. Define the getDefinitionDescription()
 * This method return the definition of your plug in: parameters, category, measures.
 * 4. Define the execute() method
 * Now you are! this is the main part of your plug in, the execution.
 * At input, TruckMilk gives you the value of all parameters the user gives. This is the value for parameters defined in the getDefinitionDescription().
 * At the output, you have to produce a status (SUCCESS, ERROR or else), reports and measures.
 * 5. Additional methods: checkPluginEnvironment() and checkJobEnvironment()
 * If you want, you can implements this method. The checkPluginEnvironment() is called at the begining. You way want to test if all additionnal information are
 * set. For example, you may need an external JAR file: it is present?
 * checkJobEnvironment() is different. It depends of the job definition, and it is executed before each execution. For example, you ask as a parameter a
 * DataSource to connect to an external database. Is this database can be accessed before the execution
 * For more information, have a look to the PDF document
 */
public class MilkPing extends MilkPlugIn {

    /**
     * define the different parameters.
     * A parameter has a type (a Date, a String), and may have condition to display it or not
     */
    private final static PlugInParameter cstParamAddDate = PlugInParameter.createInstance("addDate", "Add a date", TypeParameter.BOOLEAN, true, "If set, the date of execution is added in the status of execution");
    private final static PlugInParameter cstParamTimeExecution = PlugInParameter.createInstance("timeExecution", "Time execution (in mn)", TypeParameter.LONG, true, "The job will run this time, and will update the % of execution each minutes");

    /**
     * Jobs can create measure, to display it in a nice view
     */
    private final static PlugInMeasurement cstMesureMS = PlugInMeasurement.createInstance("MS", "Random Value", "Give a random value to demonstrate the function");
    private final static PlugInMeasurement cstMesureHourOfDay = PlugInMeasurement.createInstance("HourOfDay", "Hour of day", "Register the hour of day when the execution ran");

    private final static String LOGGER_LABEL = "MilkPing";
    private final static Logger logger = Logger.getLogger(MilkPing.class.getName());

    /**
     * TruckMilk use the BonitaEvent library to display event (which may be information or error)
     * It's very important that the user describe the error, explain it, and give an action plan. Instead to give a SQL ERROR to the user, developpers has to
     * explain what's happen, what is the consequence, and what is the action to do.
     * For each event, you have to specify:
     * - a package and a number. Then, the event is uniq and can be identified immediately by the administrator and the developper to figure out what's going on
     * - a level (INFO, ERROR, WARNING, CRITICAL)
     * - a title to display to the user. Example "Bad SQL Request"
     * - a cause : it's mandatory for an error. The SQL Request failed: what is the cause? Can't connect to the database? Bad syntax? Missing parameters?
     * - a consequence : it's mandatory for an error: explain to the user the consequence of this error. Ok, the SQL can(t be executed, so we have no history
     * for example, but the treatment can continue.
     * - an action: what the user has to do? Database is done, then action is to contact the administrator of the database behind the source.
     */
    private final static BEvent eventPing = new BEvent(MilkPing.class.getName(), 1, Level.INFO,
            "Ping !", "The ping job is executed correctly");

    private final static BEvent eventSearchUserErrors = new BEvent(MilkPing.class.getName(), 2, Level.ERROR,
            "Search Users", "Search Users failed", "List of users will be empty", "Check the exception");

    private final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

    public MilkPing() {
        super(TYPE_PLUGIN.EMBEDED);
    }

    /**
     * plug in can check its environment, to detect if you missed something. An external component may
     * be required and are not installed.
     * This call is perform when users click on "getStatus" in the maintenance panel, and before each execution.
     * 
     * @return a list of Events.
     */
    @Override
    public List<BEvent> checkPluginEnvironment(MilkJobExecution milkJobExecution) {
        return new ArrayList<>();
    }

    /**
     * check the Job's environment. This verification is executed before each execution, and all execution parameters is given at input. So the verification is
     * done with the real input.
     */
    public List<BEvent> checkJobEnvironment(MilkJobExecution milkJobExecution) {
        return new ArrayList<>();
    }

    /**
     * return the description of ping job.
     * Description contains:
     * - name (which must be uniq), explanation and label
     * - category
     * - way to ask to stop the job. Users can decide to limit the job execution in time, or in number of item processed. Of course, if you describe you can
     * stop in time, you have to implement it in your code.
     * - the list of parameters
     * - the list of measures
     */
    @Override
    public MilkPlugInDescription getDefinitionDescription(MilkJobContext milkJobContext) {
        MilkPlugInDescription milkPlugInDescription = new MilkPlugInDescription();

        milkPlugInDescription.setName("Ping");
        milkPlugInDescription.setExplanation("Just do a ping");
        milkPlugInDescription.setLabel("Ping job");
        milkPlugInDescription.setCategory(CATEGORY.OTHER);

        milkPlugInDescription.setStopJob(JOBSTOPPER.MAXMINUTES);

        milkPlugInDescription.addParameter(cstParamAddDate);
        milkPlugInDescription.addParameter(cstParamTimeExecution);

        milkPlugInDescription.addMesure(cstMesureMS);
        milkPlugInDescription.addMesure(cstMesureHourOfDay);
        return milkPlugInDescription;
    }

    /**
     * Execution of the job. Here you are!
     * You get the value on each parameters, and you have to produce a result.
     * --
     * ------------------- Input are accessible via the jobExecution object.
     * - all parameters are accessible
     * - APIAccessor are accessible too.
     * --
     * ------------------- Output
     * You have multiple mechanism.
     * - executionStatus: the status has to be give to milkJobOutput.executionStatus.
     * - number of item processed : use setNbItemsProcessed() to give the number of item processed.
     * - listEvents: the detail of execution is provided by a list of Events. Use milkJobOutput.addEvent()
     * - Measure : you can add all measure in the report too, else they are in the measure table (user need to access it separately)
     * - Chronometers: add the chronometers informations.
     * - addReportInHtml() to add a HTML sentence
     * - addReportTableBegin() / addReportTableLine() / addReportTableEnd() : give data to safe it in HTML. Table has a protection to not keep a big amount of
     * data
     * - Document output
     * --
     * ------------------- Measure
     * A measure is a number that you want to calculate and return.
     * Measure is saved in a different table, and you can save only the two last execution, but keep 1000 measure, to display a graph.
     * Two measure are automatically saved : the number of items processed and the time of execution.
     * First, you have to declare the measure in the Description :
     * PlugInMeasurement cstMesureMS = PlugInMeasurement.createInstance( <Code>, <Label>, <Explanation>);
     * > > milkPlugInDescription.addMesure( cstMesureMS );
     * then in the execution, you can set a value to the measure
     * > > milkJobOutput.setMeasure( cstMesureMS, <value>);
     * If you want to add all measure in the report, use
     * > > milkJobOutput.addMeasuresInReport(boolean keepMesureValueNotDefine, boolean withEmbededMeasure);
     * > > > > keepMesureValueNotDefine if true, a measure not defined in the execution is added in the report, with the value 0
     * > > > > withEmbededMeasure Some measure are embedded : number of items processed and time to execute. They can be added in the report.
     * --
     * ------------------- Chronometer
     * For a long execution time, you want to register the time to execute a piece of code. Then, theses value can be added in the final report
     * To Start a chronometer, use
     * > > Chronometer sleepTimeMarker = milkJobOutput.beginChronometer( <ChronometerName> );
     * and to stop it, use
     * > > milkJobOutput.endChronometer( sleepTimeMarker);
     * -
     * Chronometer save the time from Begin to End, and the number of occurrence. So, you get the final information on the total number of execution, and the
     * number of occurence.
     * Add all chronometers in the final report by
     * > > milkJobOutput.addChronometersInReport(addNumberOfOccurrence, addAverage);
     * > > > > addNumberOfOccurrence if true, the number of occurrence is added in the report
     * > > > > addAverage if true, the average is added in the report (total time / numberOfOccurent
     * --
     * ------------------- Advancement
     * To give a feedback to the user, an advancement can be calculated and send back to the user.
     * Note: the stop mechanism can stop immediately the job, and then the advancement may be stuck at final at 45% for example
     * You have two ways to give back a status to user.
     * > > milkJobOutput.setAvancementTotalStep( <totalStep>) / milkJobExecution.setAvancementStep(<StepValue>)
     * You setup the total number of step you detect. Imagine that you want to delete cases. You detect that you have to delete 454 cases. Then, set the
     * totalNumber to 454, and after each deletion, set the avancementStep to the number of case deleted.
     * or use
     * > > milkJobOutput.setAvancement( advancementInPercent )
     * then it's up to you to calculate the % of advancement.
     * To give more feedback on what's is going on, use the milkJobOutput.setAvancementInformation(String information) to set some information.
     * Please note:
     * - Truckmilk does not update in the database the advancement at each time. it do that only every X seconds (30 secondes). This is to avoid to slow down
     * the performance.
     * Imagine that you set the TotalStep to 4 412 044, and you update the advancement for each step. You don't want that Truckmilk do 4 412 044 updates in the
     * database.
     * So, if the last setAvancement() was done in the last 30 seconds, it register the value in memory only.
     * Same for the percentage: if the percentage does not change, there is no update in the database
     * --
     * ------------------ Stop mechanism
     * if your treatment take time, you have to implement a stop mechanism.
     * Truckmilk will not kill your thread, in order to not abort brutally a treatment. So you can choose when you can stop.
     * Just check the method
     * > > milkJobExecution.isStopRequired()
     * it is is true, then the stop is required
     * For your information, there are three way to stop:
     * - user explicitly require it by clicking on the stop button
     * - user specify a "maximum execution time" and your execution reach this limit
     * - user specify a "maximum item to process", and, via the setNbItemsProcessed(), you reach this number
     * --
     * ------------------- Document management
     * Your execution may need to access a document, or will create a document.
     * The report is limited in size, so if you need to produce a list of information, you should not use the Report, but produce a document (a CSV file for
     * example).
     * A document can be accessed in READ, in WRITE (you produce it) or in READ/WRITE (you update a list of information for example).
     * -
     * First, you have to declare the parameter. You have to give a file name and a content type (used by the browser when you upload the document)
     *  > > PlugInParameter cstParamMyReadDocument = PlugInParameter.createInstanceFile("readIt", "Read the document", TypeParameter.FILEREAD, null, "Read", "Read.csv", "application/CSV")
     * -
     * PlugInParameter cstParamMyWriteDocument = PlugInParameter.createInstanceFile("writeIt", "Write the document", TypeParameter.FILEWRITE, null, "Write", "Write.csv", "application/CSV")
     * -
     * PlugInParameter cstParamMyReadWriteDocument = PlugInParameter.createInstanceFile("readWriteIt", "Read-Write the document", TypeParameter.FILEREADWRITE,
     * null, "List is calculated and saved in this parameter", "ReadWrite.csv", "application/CSV")
     * so you read the document by:
     * List<BEvent> milkJobExecution.getParameterStream(<PlugInParameter>, <OutputStream>)
     * all the content is sent to out OutputStream.
     * and you write the document by :
     * milkJobOutput.setParameterStream( <PlugInParameter>, <InputStream>)
     * Note: documents are store in the database as a BLOB.
     */
    @Override
    public MilkJobOutput executeJob(MilkJobExecution milkJobExecution) {
        MilkJobOutput milkJobOutput = milkJobExecution.getMilkJobOutput();

        // if the date has to be added in the result ?
        Boolean addDate = milkJobExecution.getInputBooleanParameter(cstParamAddDate );
        Long totalMinutesExecution = milkJobExecution.getInputLongParameter(cstParamTimeExecution );
        if (totalMinutesExecution > 60)
            totalMinutesExecution = 60L;

        long total10Step = totalMinutesExecution * 6;
        String parameters = Boolean.TRUE.equals(addDate) ? "Date: " + sdf.format(new Date()) : "";

        milkJobExecution.setAvancementTotalStep(total10Step);

        for (long step10s = 0; step10s < total10Step; step10s++) {
            if (milkJobExecution.isStopRequired())
                break;
            milkJobExecution.setAvancementStep(step10s);

            Chronometer sleepTimeMarker = milkJobOutput.beginChronometer("sleepMarker");

            try {
                Thread.sleep(1000L * 10);
            } catch (InterruptedException e) {
                logger.severe(LOGGER_LABEL + "Interrupted !" + e.toString());
                Thread.currentThread().interrupt();
            }
            milkJobOutput.endChronometer(sleepTimeMarker);

        }
        long valuePoint = (System.currentTimeMillis() % 1000) - 250;
        if (valuePoint < 250)
            valuePoint = 0;
        milkJobOutput.setMeasure(cstMesureMS, valuePoint);

        milkJobOutput.setNbItemsProcessed((int) (System.currentTimeMillis() % 1000));

        Calendar c = Calendar.getInstance();
        milkJobOutput.setMeasure(cstMesureHourOfDay, c.get(Calendar.HOUR_OF_DAY));

        milkJobExecution.setAvancementStep(totalMinutesExecution);

        milkJobOutput.addReportInHtml("<h1>Ping result</h1");
        milkJobOutput.addReportTableBegin(new String[] { "User", "First Name", "Last Name" }, 30);

        IdentityAPI identityAPI = milkJobExecution.getApiAccessor().getIdentityAPI();
        SearchOptionsBuilder sob = new SearchOptionsBuilder(0, 1000);
        sob.sort(UserSearchDescriptor.USER_NAME, Order.ASC);
        try {
            SearchResult<User> searchUsers = identityAPI.searchUsers(sob.done());
            for (User user : searchUsers.getResult()) {
                milkJobOutput.addReportTableLine(new Object[] { user.getUserName(), user.getFirstName(), user.getLastName() });
            }
        } catch (SearchException e) {
            milkJobOutput.addEvent(new BEvent(eventSearchUserErrors, e, ""));
        }
        milkJobOutput.addReportTableEnd();

        // add the chronometers
        milkJobOutput.addChronometersInReport(true, true);

        // add all measure in the report too, else there are stored only in the measure part
        milkJobOutput.addMeasuresInReport(true, false);

        milkJobOutput.addEvent(new BEvent(eventPing, parameters));
        milkJobOutput.executionStatus = ExecutionStatus.SUCCESS;
        if (milkJobExecution.isStopRequired())
            milkJobOutput.executionStatus = ExecutionStatus.SUCCESSPARTIAL;

        return milkJobOutput;
    }

}
