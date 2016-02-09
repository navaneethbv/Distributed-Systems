import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;


/*
worker flow:

1. dequeue job from /pending
2. create node on /working/hash-chunkNum with timestamp
3. work on job
4. update result at /result/hash
5. remove /working/hash-chunkNum

 */


public class Worker {

    static String workerName = "sillyEngineer";
    static String jobsPath = "/pending";
    static String workingPath = "/working";
    static String resultPath = "/results";

    static String zookeeperAddress;
    static int zookeeperPort;
    static String fileserverPath = "/fileserver";
    static int fileserverPort = 8888;
    static ZkConnector zkc;

    private Socket fileserverSocket;
    private ObjectInputStream inputStream;
    private ObjectOutputStream outputStream;

    public Worker() {

    }

    public static void main(String[] args) {

        if (args.length != 3) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker zkServer clientPort workerName");
            return;
        }
        zookeeperAddress = args[0];
        zookeeperPort = Integer.parseInt(args[1]);
        workerName = args[2];

        SyncPrimitive.Queue jobs = new SyncPrimitive.Queue(zookeeperAddress, zookeeperPort, jobsPath);

        zkc = new ZkConnector();
        try {
            zkc.connect(zookeeperAddress + ":" + zookeeperPort);
        } catch (Exception e) {
            System.err.println("Could not connect to Zookeeper!");
            System.exit(1);
        }

        Worker worker = new Worker();

        int i;
        while (true) {
            try {
                System.out.println("Looking for a job...");

                // 1: dequeue job from pending
                String jobString = jobs.consume(); // take it out of the pending queue
                Job job = new Job(jobString);

                if (job.chunkNumber == 1) {
                    assert(job.chunk1.equals("pending"));
                    job.chunk1 = "working";
                } else if (job.chunkNumber == 2) {
                    assert(job.chunk2.equals("pending"));
                    job.chunk2 = "working";
                } else if (job.chunkNumber == 3) {
                    assert(job.chunk3.equals("pending"));
                    job.chunk3 = "working";
                }

                // 2: create a node at workingPath and update at results
                worker.create(job);

                System.out.println("Got job: " + job);

                // 3: work
                String answer = worker.work(job); // try to solve the job

                if (answer != null) {
                    job.result = answer;
                    job.jobStatus = "pass";
                } else {
                    job.jobStatus = "fail";
                }

                // 4: update result
                worker.publish(job);

                // 5: delete working node
                worker.delete(job);

                System.out.println("Done with job: " + job);
            } catch (Exception e) {
                System.err.println("Something bad happened!");
            }
        }
    }

    private boolean create(Job job) {

        Job existing = new Job(job.hash, 0);
        try {
            existing = new Job( new String(zkc.getZooKeeper().getData(job.resultPath(),false, null)));
        } catch (KeeperException e) {
            // path doesn't exist
            System.err.println("Result path doesn't exist for new job at " + job.resultPath());
            e.printStackTrace();
            System.exit(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
        existing.timestamp = 0; //TODO: FIX ME JAMES

        createNode(job.resultPath(), existing.serialize(), true); //update this guy with the new timestamp

        return createNode(job.workingPath(), job.serialize(), false);
    }

    private boolean createNode(String node, String data, boolean force) {
        try {
            if (zkc.getZooKeeper().exists(node, false) != null && force) {
                Stat stat = new Stat();
                zkc.getZooKeeper().getData(node, false, stat);
                zkc.getZooKeeper().setData(node, data.getBytes(), stat.getVersion());
                return true;
            }
            if (zkc.getZooKeeper().exists(node, false) == null) {
                zkc.create(node, data, CreateMode.PERSISTENT);
                return true;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean delete(Job job) {
        try {
            Stat stat = new Stat();
            zkc.getZooKeeper().getData(job.workingPath(), false, stat);
            zkc.getZooKeeper().delete(job.workingPath(), stat.getVersion());
            return true;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        return true;
    }

    private void publish(Job job) {

        System.out.println("PUBLISHING RESULTS FOR \n\t"+job.toString());

        if (job.chunkNumber == 1) {
            assert(job.chunk1.equals("working"));
            job.chunk1 = job.jobStatus;
        } else if (job.chunkNumber == 2) {
            assert(job.chunk2.equals("working"));
            job.chunk2 = job.jobStatus;
        } else if (job.chunkNumber == 3) {
            assert(job.chunk3.equals("working"));
            job.chunk3 = job.jobStatus;
        }

        Job existing = null;
        try {
            existing = new Job( new String(zkc.getZooKeeper().getData(job.resultPath(), false, null)));
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("LOADING OLD RESULTS FOR \n\t"+existing.toString());

        if (job.chunkNumber == 1) {
            existing.chunk1 = job.jobStatus;
        } else if (job.chunkNumber == 2) {
            existing.chunk2 = job.jobStatus;
        } else if (job.chunkNumber == 3) {
            existing.chunk3 = job.jobStatus;
        }
        if (existing.chunk1.equals("pass") || existing.chunk2.equals("pass") || existing.chunk3.equals("pass")) {
            existing.jobStatus = "pass";
            if(job.jobStatus.equals("pass")){
                existing.result = job.result;
            }
        } else {
            if (existing.chunk1.equals("fail") && existing.chunk2.equals("fail") && existing.chunk3.equals("fail")) {
                existing.result = "fail";
                existing.jobStatus = "fail";
            }
        }
        System.out.println("UPDATING NODE WITH \n\t"+existing.toString());
        createNode(job.resultPath(), existing.serialize(), true); // force create == update
    }


    private boolean connectToFileserver() {
        ZooKeeper zk = zkc.getZooKeeper();
        String fileserverHostname = "";

        try {
            fileserverHostname = new String(zk.getData(fileserverPath, false, null));
            fileserverSocket = new Socket(fileserverHostname, fileserverPort);
            inputStream = new ObjectInputStream(fileserverSocket.getInputStream());
            outputStream = new ObjectOutputStream(fileserverSocket.getOutputStream());
            System.out.println("Connected to fileserver!");
        } catch (Exception e) {
            System.err.println("Error: cannot connect to fileserver!");
            return false;
        }

        return true;
    }

    private String work(Job job) {

        // 1. we're going to connect to the fileserver

        while (!connectToFileserver()) {
            try {
                Thread.sleep(5000);
            } catch (Exception e) {}
        }

        // 2. we're going to get the data from the fileserver
        FileserverPacket query = new FileserverPacket(job.chunkNumber, new ArrayList<String>());
        FileserverPacket response;

        while(true) {
            try {
                outputStream.writeObject(query);
                response = (FileserverPacket) inputStream.readObject();
                inputStream.close();
                outputStream.close();
                fileserverSocket.close();
                assert (response.ok);
                System.out.println("Obtained dictionary from fileserver!");
                break;
            } catch (Exception e) {
                while (!connectToFileserver()) {
                    try {
                        Thread.sleep(5000);
                    } catch (Exception ex) {}
                }
            }
        }

        // 3. now we're gonna do the job
        int i;
        for (i=0; i<response.lines.size(); i++) {
            String word = response.lines.get(i);
            String hash = Job.getHash(word);

            // System.out.println("Working... " + word + " => " + hash + " =? " + job.hash);

            if (hash.equalsIgnoreCase(job.hash)) {
                // good
                System.out.println("Got it! " + word);
                return word;
            }
        }
        // bad
        return null;
    }


}