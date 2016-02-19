package groupby;

import groupby.aggregator.AggregateType;
import groupby.aggregator.AggregatorImple;
import groupby.event.TestEvent;

import java.util.*;

/**
 * Created by dongbin.db on 2016/2/19.
 */

public class App {
    private TestEvent createEntity(final String cluster, final String datacenter,
                                   final String rack, int numHosts, long numClusters){
        TestEvent entity = new TestEvent();
        Map<String, String> tags = new HashMap<String, String>(){{
            put("cluster", cluster);
            put("datacenter", datacenter);
            put("rack", rack);
        }};
        entity.setTags(tags);
        entity.setNumHosts(numHosts);
        entity.setNumClusters(numClusters);
        return entity;
    }

    public void testSingleGroupbyFieldSingleFunctionForCount(){
        TestEvent[] entities = new TestEvent[5];
        entities[0] = createEntity("cluster1", "dc1", "rack123", 12, 2);
        entities[1] = createEntity("cluster1", "dc1", "rack123", 20, 1);
        entities[2] = createEntity("cluster1", "dc1", "rack128", 10, 0);
        entities[3] = createEntity("cluster2", "dc1", "rack125", 9, 2);
        entities[4] = createEntity("cluster2", "dc2", "rack126", 15, 2);

        AggregatorImple agg = new AggregatorImple(Arrays.asList("cluster"), Arrays.asList(AggregateType.count),
                Arrays.asList("*"));
        try{
            for(TestEvent e : entities){
                agg.process(e);
            }
            Map<List<String>, List<Double>> result = agg.result();
            System.out.println(result.size());
            System.out.println(result.get(Arrays.asList("cluster1")).get(0));
            System.out.println(result.get(Arrays.asList("cluster2")).get(0));
        }catch(Exception ex){
            System.out.println(ex);
        }

        agg = new AggregatorImple(Arrays.asList("datacenter"), Arrays.asList(AggregateType.count), Arrays.asList("*"));
        try{
            for(TestEvent e : entities){
                agg.process(e);
            }
            Map<List<String>, List<Double>> result = agg.result();
            System.out.printf(String.valueOf(result.size())+"\n");
            System.out.println(result.get(Arrays.asList("dc1")).get(0));
            System.out.println(result.get(Arrays.asList("dc2")).get(0));
        }catch(Exception ex){
            System.out.println(ex);
        }


        agg = new AggregatorImple(new ArrayList<String>(),
                Arrays.asList(AggregateType.sum), Arrays.asList("numHosts"));
        try{
            for(TestEvent e : entities){
                agg.process(e);
            }
            Map<List<String>, List<Double>> result = agg.result();
            System.out.println(result.size());
            System.out.println(result.get(new ArrayList<String>()).get(0));
            System.out.println((double)(entities[0].getNumHosts()+entities[1].getNumHosts()+
                    entities[2].getNumHosts()+entities[3].getNumHosts()+entities[4].getNumHosts()));
        }catch(Exception ex){
            System.out.println(ex);
        }
    }
    public static void main(String[] args) {
        App test = new App();
        test.testSingleGroupbyFieldSingleFunctionForCount();
    }
}
