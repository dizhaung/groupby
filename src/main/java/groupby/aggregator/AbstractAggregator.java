package groupby.aggregator;

import groupby.event.EventBase;
import org.apache.commons.beanutils.PropertyUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dongbin.db on 2016/2/19.
 */
public abstract class AbstractAggregator implements Aggregator {

    private static final String UNASSIGNED = "unassigned";
    protected List<String> groupbyFields;
    protected List<AggregateType> aggregateTypes;
    protected List<String> aggregatedFields;
    private Boolean[] _groupbyFieldPlacementCache;
    private Method[] _aggregateFieldReflectedMethodCache;

    public AbstractAggregator(List<String> groupbyFields, List<AggregateType> aggregateFuntionTypes, List<String> aggregatedFields){
        this.groupbyFields = groupbyFields;
        this.aggregateTypes = aggregateFuntionTypes;
        this.aggregatedFields = aggregatedFields;
        _aggregateFieldReflectedMethodCache = new Method[this.aggregatedFields.size()];
        _groupbyFieldPlacementCache = new Boolean[this.groupbyFields.size()];
    }

    public abstract Object result();

    protected String createGroupFromTags(EventBase entity, String groupbyField, int i){
        String groupbyFieldValue = entity.getTags().get(groupbyField);
        if(groupbyFieldValue != null){
            _groupbyFieldPlacementCache[i] = true;
            return groupbyFieldValue;
        }
        return null;
    }

    protected String createGroupFromQualifiers(EventBase entity, String groupbyField, int i){
        try{
            PropertyDescriptor pd = PropertyUtils.getPropertyDescriptor(entity, groupbyField);
            if(pd == null)
                return null;
            _groupbyFieldPlacementCache[i] = false;
            return (String)(pd.getReadMethod().invoke(entity));
        }catch(NoSuchMethodException ex){
            return null;
        }catch(InvocationTargetException ex){
            return null;
        }catch(IllegalAccessException ex){
            return null;
        }
    }

    protected String determineGroupbyFieldValue(EventBase entity, String groupbyField, int i){
        Boolean placement = _groupbyFieldPlacementCache[i];
        String groupbyFieldValue = null;
        if(placement != null){
            groupbyFieldValue = placement.booleanValue() ? createGroupFromTags(entity, groupbyField, i) : createGroupFromQualifiers(entity, groupbyField, i);
        }else{
            groupbyFieldValue = createGroupFromTags(entity, groupbyField, i);
            if(groupbyFieldValue == null){
                groupbyFieldValue = createGroupFromQualifiers(entity, groupbyField, i);
            }
        }
        groupbyFieldValue = (groupbyFieldValue == null ? UNASSIGNED : groupbyFieldValue);
        return groupbyFieldValue;
    }

    protected List<Double> createPreAggregatedValues(EventBase entity) throws Exception{
        List<Double> values = new ArrayList<Double>();
        int functionIndex = 0;
        for(AggregateType type : aggregateTypes){
            if(type.name().equals(AggregateType.count.name())){
                values.add(new Double(1));
            }else{
                String aggregatedField = aggregatedFields.get(functionIndex);
                try {
                    Method m = _aggregateFieldReflectedMethodCache[functionIndex];
                    if (m == null) {
                        String tmp = aggregatedField.substring(0, 1).toUpperCase() + aggregatedField.substring(1);
                        m = entity.getClass().getMethod("get" + tmp);
                        _aggregateFieldReflectedMethodCache[functionIndex] = m;
                    }
                    Object obj = m.invoke(entity);
                    values.add(numberToDouble(obj));
                } catch (Exception ex) {
                    throw ex;
                }
            }
            functionIndex++;
        }
        return values;
    }

    protected Double numberToDouble(Object obj) throws Exception {
        if(obj instanceof Double)
            return (Double)obj;
        if(obj instanceof Integer){
            return new Double(((Integer)obj).doubleValue());
        }
        if(obj instanceof Long){
            return new Double(((Long)obj).doubleValue());
        }
        if(obj == null){
            return new Double(0.0);
        }
        if(obj instanceof String){
            try{
                return new Double((String)obj);
            }catch(Exception ex){
                System.out.println("Datapoint ignored because it can not be converted to correct number for " + obj + ex);
                return new Double(0.0);
            }
        }

        throw new Exception(obj.getClass().toString() + " type is not support. The aggregated field must be numeric type, int, long or double");
    }
}
