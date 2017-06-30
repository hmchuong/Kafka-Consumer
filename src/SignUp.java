import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by hmchuong on 28/06/2017.
 */
public class SignUp {
    public String project_id;
    public String json;
    public Integer timeZone;

    public SignUp(String json){
        Configuration config = Configuration.builder().build();
        config.addOptions(Option.SUPPRESS_EXCEPTIONS);
        Object document = config.jsonProvider().parse(json);
        /*
        String date = JsonPath.read(document, "$._sent_at");
        //date = date.split("\"")[1];
        DateFormat df = new SimpleDateFormat("'Date(\"'yyyy-MM-dd'T'HH:mm:ss.SSSXXX'\")'");
        try {
            this.sent_at = df.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        */
        this.json = json;
        try {
            this.project_id = JsonPath.read(document, "$.project_id.$oid");
        }catch (PathNotFoundException e){
            this.project_id = "";
            e.printStackTrace();
        }

        try {
            this.timeZone = JsonPath.read(document, "$.timezone");
        }catch (PathNotFoundException e){
            this.timeZone = null;
        }
    }

    public String toJson(){
        if (timeZone != null){
            return json.substring(0,json.indexOf("{")+1)+"\"timezone\":"+String.valueOf(timeZone)+","+json.substring(json.indexOf("{")+1);
        }
        return json;
    }
}
