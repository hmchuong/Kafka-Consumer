import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;

/**
 * Created by hmchuong on 28/06/2017.
 */
public class Event {
    private String projectId;
    private String json;
    private Integer timeZone = null;
    private String name;

    public String getProjectId() {
        return projectId;
    }

    public void setTimeZone(Integer timeZone) {
        this.timeZone = timeZone;
    }

    public Integer getTimeZone() {
        return timeZone;
    }

    public Event(String json){
        Configuration config = Configuration.builder().build();
        config.addOptions(Option.SUPPRESS_EXCEPTIONS);
        Object document = config.jsonProvider().parse(json);
        this.json = json;

        // Read projectId
        try {
            this.projectId = JsonPath.read(document, "$.project_id.$oid");
        }catch (PathNotFoundException e){
            this.projectId = "";
            e.printStackTrace();
        }

        // Read name
        try {
            this.name = JsonPath.read(document,"$.name");
        }catch (PathNotFoundException e){
            this.name = null;
        }
    }

    public String toString(){
        if (timeZone != null){
            return json.substring(0,json.indexOf("{")+1)+"\"timezone\":"+String.valueOf(timeZone)+","+json.substring(json.indexOf("{")+1);
        }
        return json;
    }

    /** Link prefix with name of the event
     * @param prefix - prefix of the topic
     * @return
     */
    public String getOutTopic(String prefix){
        if (name != null){
            return prefix+name;
        }
        return null;
    }
}