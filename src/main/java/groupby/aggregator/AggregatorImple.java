package groupby.aggregator;

import groupby.bucket.GroupbyBucket;
import groupby.event.EventBase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by dongbin.db on 2016/2/19.
 */
public class AggregatorImple extends AbstractAggregator{
    protected GroupbyBucket bucket;

    public AggregatorImple(List<String> groupbyFields, List<AggregateType> aggregateFuntionTypes, List<String> aggregatedFields){
        super(groupbyFields, aggregateFuntionTypes, aggregatedFields);
        bucket = new GroupbyBucket(this.aggregateTypes);
    }

    public void process(EventBase entity) throws Exception{
        List<String> groupbyFieldValues = createGroup(entity);
        List<Double> preAggregatedValues = createPreAggregatedValues(entity);
        bucket.addDatapoint(groupbyFieldValues, preAggregatedValues);
    }

    public Map<List<String>, List<Double>> result(){
        return bucket.result();
    }

    protected List<String> createGroup(EventBase entity){
        List<String> groupbyFieldValues = new ArrayList<String>();
        int i = 0;
        for(String groupbyField : groupbyFields){
            String groupbyFieldValue = determineGroupbyFieldValue(entity, groupbyField, i++);
            groupbyFieldValues.add(groupbyFieldValue);
        }
        return groupbyFieldValues;
    }
}

