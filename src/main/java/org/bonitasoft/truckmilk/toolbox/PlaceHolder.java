package org.bonitasoft.truckmilk.toolbox;

import java.util.HashMap;
import java.util.Map;

import org.bonitasoft.engine.bpm.flownode.HumanTaskInstance;

public class PlaceHolder {

    /**
     * @param humanTask
     * @param bonitaHeader something like "http://localhost:7070"
     * @return
     */

    public static Map<String, Object> getPlaceHolder(HumanTaskInstance humanTask, String bonitaHeader) {
        Map<String, Object> placeHolder = new HashMap<String, Object>();
        placeHolder.put("caseId", String.valueOf(humanTask.getParentProcessInstanceId()));
        String taskUrl = "<a href=\"";
        taskUrl += bonitaHeader;
        taskUrl += "/bonita/portal/resource/taskInstance/";
        taskUrl += humanTask.getId();
        taskUrl += "\">Task</a>";
        placeHolder.put("taskUrl", taskUrl);
        return placeHolder;
    }

    /**
     * from a string with a Place Holder, retrieve all the placeHolder and replace it
     * Example, string is "hello mister {{name}}, welcome to {{city}}"
     * in the map, you have "name" : "walter", "city":"San Francisco".
     * Result is "hello mister walter, welcome to San Francisco"
     */

    public static String replacePlaceHolder(Map<String, Object> placeHolder, String content) {
        // search all place Holder
        int foundBegin = 0;
        int foundEnd = 0;
        do {
            foundBegin = content.indexOf("{{");
            foundEnd = -1;
            if (foundBegin >= 0)
                foundEnd = content.indexOf("}}", foundBegin);
            if (foundEnd >= 0) {
                // replace !
                String index = content.substring(foundBegin + 2, foundEnd);
                Object value = placeHolder.get(index);
                if (value == null)
                    value = "";
                content = content.substring(0, foundBegin) + value.toString() + content.substring(foundEnd + 2);
            }
        } while (foundBegin >= 0 && foundEnd >= 0);
        return content;

    }
}
