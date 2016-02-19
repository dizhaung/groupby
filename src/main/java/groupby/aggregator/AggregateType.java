package groupby.aggregator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by dongbin.db on 2016/2/19.
 */
public enum AggregateType {
    count("^(count)$"),
    sum("^sum\\((.*)\\)$"),
    avg("^avg\\((.*)\\)$"),
    max("^max\\((.*)\\)$"),
    min("^min\\((.*)\\)$");

    private Pattern pattern;
    private AggregateType(String patternString){
        this.pattern = Pattern.compile(patternString);
    }

    public AggregateTypeMatcher matcher(String function){
        Matcher m = pattern.matcher(function);

        if(m.find()){
            return new AggregateTypeMatcher(this, true, m.group(1));
        }else{
            return new AggregateTypeMatcher(this, false, null);
        }
    }

    public static AggregateTypeMatcher matchAll(String function){
        for(AggregateType type : values()){
            Matcher m = type.pattern.matcher(function);
            if(m.find()){
                return new AggregateTypeMatcher(type, true, m.group(1));
            }
        }
        return new AggregateTypeMatcher(null, false, null);
    }
}


class AggregateTypeMatcher {
    private final AggregateType type;
    private final boolean matched;
    private final String field;

    public AggregateTypeMatcher(AggregateType type, boolean matched, String field){
        this.type = type;
        this.matched = matched;
        this.field = field;
    }

    public boolean find(){
        return this.matched;
    }

    public String field(){
        return this.field;
    }

    public AggregateType type(){
        return this.type;
    }
}
