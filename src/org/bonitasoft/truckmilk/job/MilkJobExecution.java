package org.bonitasoft.truckmilk.job;

import java.io.OutputStream;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bonitasoft.truckmilk.engine.MilkPlugIn;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugInParameter;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.PlugTourOutput;
import org.bonitasoft.truckmilk.engine.MilkPlugIn.TypeParameter;
import org.bonitasoft.truckmilk.toolbox.TypesCast;

/* ******************************************************************************** */
/*                                                                                  */
/* Execution */
/*                                                                                  */
/*                                                                                  */
/* ******************************************************************************** */

public class MilkJobExecution {

    public String tourName;
    // this parameters is initialised with the value in the PlugTour, and may change after
    public Map<String, Object> tourParameters;

    private MilkJob milkJob;

    public MilkJobExecution(MilkJob milkJob) {
        this.milkJob = milkJob;
        this.tourParameters = milkJob.getTourParameters();
        tourName = milkJob.getName();
    }

    public PlugTourOutput getPlugTourOutput() {
        return new PlugTourOutput(milkJob);
    }

    /**
     * Boolean
     * 
     * @param parameter
     * @return
     */
    public Boolean getInputBooleanParameter(PlugInParameter parameter) {
        return getInputBooleanParameter(parameter.name, (Boolean) parameter.defaultValue);
    }

    public Boolean getInputBooleanParameter(String name, Boolean defaultValue) {
        return TypesCast.getBoolean(tourParameters.get(name).toString(), defaultValue);

    }

    /**
     * Long
     * 
     * @param parameter
     * @return
     */
    public Long getInputLongParameter(PlugInParameter parameter) {
        return getInputLongParameter(parameter.name, parameter.defaultValue == null ? null : Long.valueOf(parameter.defaultValue.toString()));
    }

    public Long getInputLongParameter(String name, Long defaultValue) {
        return TypesCast.getLong(tourParameters.get(name), defaultValue);
    }

    public String getInputStringParameter(PlugInParameter parameter) {
        return getInputStringParameter(parameter.name, (String) parameter.defaultValue);
    }

    public String getInputStringParameter(String name, String defaultValue) {
        return TypesCast.getString(tourParameters.get(name), defaultValue);
    }

    public void getStreamParameter(PlugInParameter plugInParameter, OutputStream output) {

        if (plugInParameter.typeParameter == TypeParameter.FILEREAD || plugInParameter.typeParameter == TypeParameter.FILEREADWRITE || plugInParameter.typeParameter == TypeParameter.FILEWRITE) {
            // update the PLUGIN parameters
            milkJob.getParameterStream(plugInParameter, output);
        } else {
            MilkPlugIn.logger.severe("setParameterStream not allowed on parameter[" + plugInParameter.name + "] (plugin " + milkJob.getPlugIn().getName() + "]");
        }
    }

    public List<?> getInputListParameter(PlugInParameter parameter) {
        return getInputListParameter(parameter.name, (List<?>) parameter.defaultValue);
    }

    public List<?> getInputListParameter(String name, List<?> defaultValue) {
        try {
            return (List<?>) tourParameters.get(name);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> getInputListMapParameter(PlugInParameter parameter) {
        return (List<Map<String, Object>>) getInputListParameter(parameter.name, (List<Map<String, Object>>) parameter.defaultValue);
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> getInputListMapParameter(String name, List<Map<String, Object>> defaultValue) {
        try {
            return (List<Map<String, Object>>) tourParameters.get(name);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* please Stop mechanism */
    /*                                                                                  */
    /* The please stop mechanism can be setup by a maximum time, maximum item process */
    /* or via the interface by an adminitrator. Plugin are suppose to check the */
    /*
     * pleaseStop() method
     * /*
     */
    /* ******************************************************************************** */
    private Long pleaseStopInMinutes = null;
    private Long pleaseStopInManagedItems = null;
    private long nbManagedItems = 0;

    public void setPleaseStopAfterTime(long timeInMinutes, Long defaultValue) {
        this.pleaseStopInMinutes = timeInMinutes == 0 ? defaultValue : timeInMinutes;
    }

    public Long getPleaseStopAfterManagerTime() {
        return pleaseStopInMinutes;
    }

    /**
     * in advancement, there are 2 concepts:
     * - the number of step advance
     * - the number of item managed
     * Example : you have a list of cases potentialy to delete in a list of 2000. Not all lines will have a case to delete.
     * If you setup the number of managedItem to 400, you want to stop after 400 deletions.
     * 
     * @param nbStepManagedItem
     */

    public void setPleaseStopAfterManagedItems(Long nbManagedItems, Long defaultValue) {
        pleaseStopInManagedItems = nbManagedItems == null || nbManagedItems==0 ? defaultValue : nbManagedItems;
    }

    public Long getPleaseStopAfterManagerItems() {
        return pleaseStopInManagedItems;
    }

    public void addManagedItem(long managedItems) {
        this.nbManagedItems += managedItems;
    }

    boolean pleaseStop = false;

    public void setPleaseStop(boolean pleaseStop) {
        this.pleaseStop = pleaseStop;
    }

    public boolean pleaseStop() {
        if (pleaseStop)
            return true;
        if (milkJob.askForStop())
            return true;
        if (pleaseStopInMinutes != null) {
            long currentTime = System.currentTimeMillis();
            long timeInMn = (currentTime - startTimeMs) / 1000 / 60;
            if (timeInMn > pleaseStopInMinutes) {
                return true;
            }
        }
        if (pleaseStopInManagedItems != null) {
            if (nbManagedItems > pleaseStopInManagedItems) {
                return true;
            }
        }
        // no reason to stop
        return false;

    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Advancement update */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    public long totalStep;

    /**
     * Plug in can set an % of advancement if it want
     * 
     * @param advancementInPercent
     */
    public void setAvancementTotalStep(long totalStep) {
        this.totalStep = totalStep;
        // register in the tour the % of advancement, and then calculated an estimated end date.
    }

    /**
     * Plug in can set an % of advancement if it want
     * 
     * @param advancementInPercent
     */
    public void setAvancementStep(long step) {
        if (totalStep != 0)
            setAvancement((int) (100*step / totalStep));
        // register in the tour the % of advancement, and then calculated an estimated end date.
    }

    /**
     * /**
     * Plug in can set an % of advancement if it want
     * 
     * @param advancementInPercent
     */
    public void setAvancement(int advancementInPercent) {
        if (advancementInPercent != milkJob.trackExecution.percent) {
            // update
            milkJob.trackExecution.percent = advancementInPercent;
            long timeExecution = System.currentTimeMillis() - startTimeMs;
            if (timeExecution > 0 && advancementInPercent > 0) {
                milkJob.trackExecution.timeEstimatedInMs = timeExecution * 100 / advancementInPercent;
                milkJob.trackExecution.endDateEstimated = new Date( milkJob.trackExecution.timeEstimatedInMs + startTimeMs );
                // save the current advancement
                milkJob.milkJobFactory.dbSaveJob(milkJob, false);
            }
        }
        // register in the tour the % of advancement, and then calculated an estimated end date.
    }

    private long startTimeMs;

    /**
     * plug in can overwrite this method, but please call this
     */
    public void start() {
        startTimeMs = System.currentTimeMillis();
        pleaseStop = false;

        milkJob.isImmediateExecution = false;
        milkJob.trackExecution.inExecution = true;
        milkJob.trackExecution.startTime = startTimeMs;
        milkJob.trackExecution.percent = 0;
        milkJob.trackExecution.timeEstimatedInMs = -1;
        milkJob.trackExecution.endDateEstimated = null;

    }

    public void end() {
        milkJob.trackExecution.inExecution = false;
        milkJob.trackExecution.percent = 100;
        milkJob.trackExecution.timeEstimatedInMs = 0;
        milkJob.trackExecution.endDateEstimated = new Date();

    }

    public long getTimeFromStartupInMs() {
        return System.currentTimeMillis() - startTimeMs;
    }
}
