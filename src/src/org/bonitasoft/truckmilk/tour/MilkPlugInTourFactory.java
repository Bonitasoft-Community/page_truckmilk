package org.bonitasoft.truckmilk.tour;

import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.ext.properties.BonitaProperties;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.truckmilk.plugin.MilkPlugIn.PlugTourOutput;

public class MilkPlugInTourFactory {

  public static BEvent EVENT_TOUR_NOT_FOUND = new BEvent(MilkPlugInTourFactory.class.getName(), 3, Level.APPLICATIONERROR,
      "Tour not found",
      "The Tour is not found, the operation is not performed.Maybe someone else delete it before you?",
      "Operation is not done", "Refresh the page");

  public static BEvent eventTourAlreadyExist = new BEvent(MilkPlugInTourFactory.class.getName(), 2, Level.APPLICATIONERROR,
      "Tour Already Exist", "The Tour already exist with this name, a tour must have a uniq name",
      "Tour is not register", "Choose a different name");

  private static BEvent eventBadTourJsonFormat = new BEvent(MilkPlugInTourFactory.class.getName(), 3,
      Level.APPLICATIONERROR,
      "Bad tour format", "A Milk tour can't be read due to a Json format", "This tour is lost", "Reconfigure it");

  public static MilkPlugInTourFactory milkPlugInTourFactory = new MilkPlugInTourFactory();

  public static MilkPlugInTourFactory getInstance() {
    return milkPlugInTourFactory;
  }

  // keep the list of all tour
  Map<Long, MilkPlugInTour> listToursId = new HashMap<Long, MilkPlugInTour>();

  /*
   * Map<String, MilkPlugInTour> listToursName = new HashMap<String, MilkPlugInTour>();
   * public MilkPlugInTour getByName(String name)
   * {
   * return listToursName.get( name==null ? MilkPlugInTour.DEFAULT_NAME : name );
   * }
   * public Map<String, MilkPlugInTour> getMapTourName()
   * {
   * return listToursName;
   * }
   */
  public MilkPlugInTour getById(Long id) {
    return listToursId.get(id);
  }

  public Map<Long, MilkPlugInTour> getMapTourId() {
    return listToursId;
  }

  /**
   * check if plug in has all the environment they need
   * 
   * @param tenantId
   * @return
   */
  public List<BEvent> checkEnvironment(long tenantId) {
    List<BEvent> listEvents = new ArrayList<BEvent>();
    return listEvents;
  }

  public List<BEvent> removeTour(long idTour, long tenantId) {
    List<BEvent> listEvents = new ArrayList<BEvent>();
    if (!listToursId.containsKey(idTour)) {
      listEvents.add(new BEvent(EVENT_TOUR_NOT_FOUND, "Tour[" + idTour + "]"));
      return listEvents;
    }

    listEvents.addAll(dbRemovePlugInTour(listToursId.get(idTour), tenantId));
    listToursId.remove(idTour);
    return listEvents;
  }

  public synchronized List<BEvent> registerATour(MilkPlugInTour plugInTour) {
    List<BEvent> listEvents = new ArrayList<BEvent>();
    // name must be unique
    for (MilkPlugInTour plugI : listToursId.values()) {
      if (plugI.getName().equals(plugInTour.getName())) {
        listEvents.add(new BEvent(eventTourAlreadyExist, plugInTour.getName()));
        return listEvents;
      }
    }
    listToursId.put(plugInTour.getId(), plugInTour);
    return listEvents;
  }
  // save a tour

  // read a tour
  public List<BEvent> dbLoadAllPlugInTour(long tenantId, MilkCmdControl milkCmdControl) {
    List<BEvent> listEvents = new ArrayList<BEvent>();
    BonitaProperties bonitaProperties = new BonitaProperties(BonitaPropertiesName, tenantId);
    listEvents.addAll(bonitaProperties.loaddomainName(BonitaPropertiesDomain));
    List<String> listToursToDelete = new ArrayList<String>();
    Enumeration<?> enumKey = bonitaProperties.propertyNames();
    boolean newIdWasGenerated = false;
    while (enumKey.hasMoreElements()) {
      String idTour = (String) enumKey.nextElement();
      String plugInTourSt = (String) bonitaProperties.get(idTour);
      MilkPlugInTour plugTour = MilkPlugInTour.getInstanceFromJson(plugInTourSt, milkCmdControl);
      if (plugTour != null) {
        if (plugTour.newIdGenerated)
          newIdWasGenerated = true;
        listToursId.put(plugTour.getId(), plugTour);
      } else {
        listToursToDelete.add(idTour);
        listEvents.add(new BEvent(eventBadTourJsonFormat, "Id[" + idTour + "]"));
      }
    }
    if (listToursToDelete.size() > 0 || newIdWasGenerated) {
      for (String idTour : listToursToDelete)
        bonitaProperties.remove(idTour);
      // save it to delete all corrupt tour
      listEvents.addAll(bonitaProperties.store());
    }
    return listEvents;
  }

  public List<BEvent> dbSaveAllPlugInTour(long tenantId) {
    List<BEvent> listEvents = new ArrayList<BEvent>();
    BonitaProperties bonitaProperties = new BonitaProperties(BonitaPropertiesName, tenantId);
    listEvents.addAll(bonitaProperties.loaddomainName(BonitaPropertiesDomain));
    for (MilkPlugInTour plugInTour : listToursId.values()) {
      bonitaProperties.put(plugInTour.getId(), plugInTour.getJsonSt());
    }
    listEvents.addAll(bonitaProperties.store());

    return listEvents;
  }

  /**
   * load a particular plugtour
   * 
   * @param name
   * @return
   */

  public MilkPlugInTour dbLoadPlugInTour(Long idTour, long tenantId, MilkCmdControl milkCmdControl) {
    List<BEvent> listEvents = new ArrayList<BEvent>();
    BonitaProperties bonitaProperties = new BonitaProperties(BonitaPropertiesName, tenantId);
    listEvents.addAll(bonitaProperties.loaddomainName(BonitaPropertiesDomain));

    String plugInTourSt = (String) bonitaProperties.get(idTour.toString());
    MilkPlugInTour plugInTour = MilkPlugInTour.getInstanceFromJson(plugInTourSt, milkCmdControl);
    listToursId.put(plugInTour.getId(), plugInTour);

    if (plugInTour.newIdGenerated) {
      bonitaProperties.put(plugInTour.getId(), plugInTour.getJsonSt());

      listEvents.addAll(bonitaProperties.store());

    }

    return plugInTour;

  }

  /**
   * save a particular plug tour
   * 
   * @param tour
   * @return
   */
  public List<BEvent> dbSavePlugInTour(MilkPlugInTour plugInTour, long tenantId) {
    List<BEvent> listEvents = new ArrayList<BEvent>();

    BonitaProperties bonitaProperties = new BonitaProperties(BonitaPropertiesName, tenantId);
    listEvents.addAll(bonitaProperties.loaddomainName(BonitaPropertiesDomain));

    bonitaProperties.put(String.valueOf(plugInTour.getId()), plugInTour.getJsonSt());

    listEvents.addAll(bonitaProperties.store());

    return listEvents;
  }

  /**
   * remove a particular plug tour
   * 
   * @param tour
   * @return
   */
  private List<BEvent> dbRemovePlugInTour(MilkPlugInTour plugInTour, long tenantId) {
    List<BEvent> listEvents = new ArrayList<BEvent>();
    BonitaProperties bonitaProperties = new BonitaProperties(BonitaPropertiesName, tenantId);
    listEvents.addAll(bonitaProperties.loaddomainName(BonitaPropertiesDomain));

    // remove(pluginTour.getId() does not work, so let's compare the key
    for (Object key : bonitaProperties.keySet())
    {
      if ( Long.valueOf( key.toString() ).equals(plugInTour.getId()))
          bonitaProperties.remove( key );
    }

    listEvents.addAll(bonitaProperties.store());

    return listEvents;
  }

  public final static String BonitaPropertiesName = "MilkTour";
  private final static String BonitaPropertiesDomain = "tour";

  // save a tour execution
  public void saveExecution(Date currentDate, PlugTourOutput output) {

  }

  // load all tour execution

}
