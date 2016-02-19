package groupby.event;

/**
 * Created by dongbin.db on 2016/2/19.
 */
public class TestEvent extends EventBase {
    private int numHosts;
    private Long numClusters;

    public int getNumHosts() {
        return numHosts;
    }

    public void setNumHosts(int numHosts) {
        this.numHosts = numHosts;
    }

    public Long getNumClusters() {
        return numClusters;
    }

    public void setNumClusters(Long numClusters) {
        this.numClusters = numClusters;
    }
    public String toString(){
        StringBuffer sb = new StringBuffer();
        sb.append(super.toString());
        return sb.toString();
    }
}
