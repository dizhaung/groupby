package groupby.bucket;

import groupby.aggregator.AggregateType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by dongbin.db on 2016/2/19.
 */
public class GroupbyBucket {

    public static Map<String, FunctionFactory> functionFactories =
            new HashMap<String, FunctionFactory>();

    static{
        functionFactories.put(AggregateType.count.name(), new CountFactory());
        functionFactories.put(AggregateType.sum.name(), new SumFactory());
        functionFactories.put(AggregateType.min.name(), new MinFactory());
        functionFactories.put(AggregateType.max.name(), new MaxFactory());
        functionFactories.put(AggregateType.avg.name(), new AvgFactory());
    }

    private List<AggregateType> types;

    private Map<List<String>, List<Function>> group2FunctionMap = new HashMap<List<String>, List<Function>>();

    public GroupbyBucket(List<AggregateType> types){
        this.types = types;
    }

    public void addDatapoint(List<String> groupbyFieldValues, List<Double> values){
        List<Function> functions = group2FunctionMap.get(groupbyFieldValues);
        if(functions == null){
            functions = new ArrayList<Function>();
            for(AggregateType type : types){
                functions.add(functionFactories.get(type.name()).createFunction());
            }
            group2FunctionMap.put(groupbyFieldValues, functions);
        }
        int functionIndex = 0;
        for(Double v : values){
            functions.get(functionIndex).run(v);
            functionIndex++;
        }
    }

    public Map<List<String>, List<Double>> result(){
        Map<List<String>, List<Double>> result = new HashMap<List<String>, List<Double>>();
        for(Map.Entry<List<String>, List<Function>> entry : this.group2FunctionMap.entrySet()){
            List<Double> values = new ArrayList<Double>();
            for(Function f : entry.getValue()){
                values.add(f.result());
            }
            result.put(entry.getKey(), values);
        }
        return result;
    }

    public static interface FunctionFactory{
        public Function createFunction();
    }

    public static abstract class Function{
        protected int count;

        public abstract void run(double v);
        public abstract double result();
        public int count(){
            return count;
        }
        public void incrCount(){
            count ++;
        }
    }

    private static class CountFactory implements FunctionFactory{
        @Override
        public Function createFunction(){
            return new Count();
        }
    }


    private static class Count extends Sum{
        public Count(){
            super();
        }
    }

    private static class SumFactory implements FunctionFactory{
        @Override
        public Function createFunction(){
            return new Sum();
        }
    }

    private static class Sum extends Function{
        private double summary;
        public Sum(){
            this.summary = 0.0;
        }
        @Override
        public void run(double v){
            this.incrCount();
            this.summary += v;
        }

        @Override
        public double result(){
            return this.summary;
        }
    }

    private static class MinFactory implements FunctionFactory{
        @Override
        public Function createFunction(){
            return new Min();
        }
    }
    public static class Min extends Function{
        private double minimum;
        public Min(){
            this.minimum = Double.MAX_VALUE;
        }

        @Override
        public void run(double v){
            if(v < minimum){
                minimum = v;
            }
            this.incrCount();
        }

        @Override
        public double result(){
            return minimum;
        }
    }

    private static class MaxFactory implements FunctionFactory{
        @Override
        public Function createFunction(){
            return new Max();
        }
    }
    public static class Max extends Function{
        private double maximum;
        public Max(){
            this.maximum = 0.0;
        }
        @Override
        public void run(double v){
            if(v > maximum){
                maximum = v;
            }
            this.incrCount();
        }

        @Override
        public double result(){
            return maximum;
        }
    }

    private static class AvgFactory implements FunctionFactory{
        @Override
        public Function createFunction(){
            return new Avg();
        }
    }
    public static class Avg extends Function{
        private double total;
        public Avg(){
            this.total = 0.0;
        }
        @Override
        public void run(double v){
            total += v;
            this.incrCount();
        }
        @Override
        public double result(){
            return this.total/this.count;
        }
    }
}
