package groupby.aggregator;

import groupby.event.EventBase;

/**
 * Created by dongbin.db on 2016/2/19.
 */
public interface Aggregator {
    public void process(EventBase event) throws Exception;
}
