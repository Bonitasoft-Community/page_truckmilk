package org.bonitasoft.truckmilk.sonarqube;

public class CheckSonarQube {

    String isEventValid;
    String pEventStatus = null;

    public boolean verify() {

        if (isEventValid.equals("N") || (pEventStatus != null && pEventStatus.equalsIgnoreCase("Complete")))
            return true;
        else
            return false;
    }
    public boolean verify2() {

        if (isEventValid.equals("N") || pEventStatus.equalsIgnoreCase("Complete"))
            return true;
        else
            return false;
    }
}
