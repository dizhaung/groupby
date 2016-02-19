package groupby.event;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by dongbin.db on 2016/2/19.
 */
public class EventBase implements Serializable {

    private long timestamp;
    private Map<String, String> tags;

    public EventBase(){
    }
    public long getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    public Map<String, String> getTags() {
        return tags;
    }
    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public String toString(){
        StringBuffer sb = new StringBuffer();
        sb.append("prefix:");
        sb.append(", timestamp:");
        sb.append(timestamp);
        sb.append(", humanReadableDate:");
        sb.append(timestamp);
        sb.append(", tags: ");
        if(tags != null){
            for(Map.Entry<String, String> entry : tags.entrySet()){
                sb.append(entry.toString());
                sb.append(",");
            }
        }
        sb.append(", encodedRowkey:");
        return sb.toString();
    }


}
