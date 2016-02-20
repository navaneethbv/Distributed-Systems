import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

/** 
 * 
 * Created by Navaneeth Rao
 */
public class DummyJobSubmitter {

    static String jobsPath = "/pending";

    public static void main(String args[]) throws KeeperException, InterruptedException {

        String word = "householders";
        String right = Job.getHash(word);

        Job j = new Job(right, 0);
        j.hash = right;
        j.chunkNumber = 1;

        ZkConnector zkc = new ZkConnector();
        try {
            zkc.connect("localhost:2181");
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        System.out.println(j.serialize());


        // 0. set up path

        Stat s = zkc.getZooKeeper().exists("/results", false);
        if (s == null) {
            zkc.getZooKeeper().create("/results", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        }

        s = zkc.getZooKeeper().exists("/working", false);
        if (s == null) {
            zkc.getZooKeeper().create("/working", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        }

        // 1. make it at result

        s = zkc.getZooKeeper().exists(j.resultPath(), false);
        if (s != null) {
            zkc.getZooKeeper().getData(j.resultPath(), false, s);
            zkc.getZooKeeper().delete(j.resultPath(),s.getVersion());
        }

        s = zkc.getZooKeeper().exists(j.resultPath(), false);
        if (s == null) {
            zkc.getZooKeeper().create(j.resultPath(),j.serialize().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        // 2. enqueue it
        SyncPrimitive.Queue jobs = new SyncPrimitive.Queue("localhost", 2181, jobsPath);

        try {
            jobs.produce(j.serialize(), right+"-"+j.chunkNumber);
        } catch (Exception e) {
            System.err.println("Test: error putting job up");
        }

        // 3. get it at results

        while (true) {
            Thread.sleep(5000);
            Job done = new Job( new String(zkc.getZooKeeper().getData(j.resultPath(), false, null)));
            System.out.println("Job is now " + done.result);
        }
    }



}
