import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;


public class JobTracker 
{

    String myPath = "/jobtracker";
    String pendingPath = "/pending";
    String workingPath = "/working";
    String resultsPath = "/results";
    ZkConnector zkc;
    Watcher watcher;

    private static String dictionaryPath;
    private static String zookeeperAddress;
    private ServerSocket serverSocket;
    private final int serverSocketPort = 8889;
    private static String myAddress;

    public static void main(String[] args) 
    {
        if (args.length != 1) 
        {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Jobtracker zkServer:clientPort");
            return;
        }

        zookeeperAddress = args[0];

        try {
            myAddress = java.net.InetAddress.getLocalHost().getCanonicalHostName();
        } catch (Exception e) { System.err.println("Fileserver: Cannot obtain own hostname"); }

        JobTracker jt = new JobTracker(zookeeperAddress);

        System.out.println("Sleeping...");
        try {
            Thread.sleep(5000);
        } catch (Exception e) {}

        jt.checkpath();

        System.out.println("Sleeping...");
        while (true) {
            try{ Thread.sleep(5000); } catch (Exception e) {}
        }
    }

    public JobTracker(String hosts) {
        zkc = new ZkConnector();
        try {
            zkc.connect(hosts);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        watcher = new Watcher() { // Anonymous Watcher
            @Override
            public void process(WatchedEvent event) {
                handleEvent(event);

            } };
    }

    private void checkpath() {
        Stat stat = zkc.exists(myPath, watcher);
        if (stat == null) {              // znode doesn't exist; let's try creating it
            System.out.println("Creating " + myPath);
            KeeperException.Code ret = zkc.create(
                    myPath,         // Path of znode
                    myAddress,           // Data is my address.
                    CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
            );
            if (ret == KeeperException.Code.OK) System.out.println("I am the primary jobtracker!");

            // got the job, now open up and listen
            try {
                serverSocket = new ServerSocket(serverSocketPort);
            } catch (IOException e) {
                System.err.println("Jobtracker failed to establish server socket on " + serverSocketPort);
                System.exit(1);
            }

            // keep listening
            while (true) {
                System.out.println("Jobtracker: I'm listening and listening and listening at " + serverSocket.getInetAddress().getCanonicalHostName());
                try {
                    Socket sock = serverSocket.accept();
                    System.out.println("A sucker connected from " + sock.getInetAddress().getCanonicalHostName());
                    JobTrackerHandler handler = new JobTrackerHandler(sock, zkc);
                    new Thread(handler).start();
                } catch (IOException e) {
                    System.err.println("Jobtracker failed to accept socket connection");
                }
            }
        }
    }

    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        Watcher.Event.EventType type = event.getType();
        if(path.equalsIgnoreCase(myPath)) {
            if (type == Watcher.Event.EventType.NodeDeleted) {
                System.out.println(myPath + " deleted! Other guy is down!");
                checkpath(); // try to become the boss
            }
            if (type == Watcher.Event.EventType.NodeCreated) {
                System.out.println(myPath + " Primary jobtracker created!");
                try{ Thread.sleep(5000); } catch (Exception e) {}
                checkpath(); // re-enable the watch
            }
        }
    }

}


class JobTrackerHandler implements Runnable {
    static String jobsPath = "/pending";
    private Socket mySock;
    private String dictionaryPath;
    private ArrayList<String> lines;

    public ObjectInputStream inStream;
    public ObjectOutputStream outStream;

    static int instanceCounter = 0;

    ZkConnector zkc;

    private String identifier;

    public JobTrackerHandler(Socket sock, ZkConnector zkc) {
        this.mySock = sock;
        this.zkc = zkc;
        identifier = "JobTracker Handler " + instanceCounter + ": ";
        instanceCounter++;
    }

    public void run () {
        try {
            outStream = new ObjectOutputStream(mySock.getOutputStream());
            outStream.flush();
            inStream = new ObjectInputStream(mySock.getInputStream());
        } catch (IOException e) {
            System.out.println(identifier + "input/output stream setup failed! Exiting thread");
            e.printStackTrace();
        }

        while (true) {
            // Load the packet from client.
            JobTrackerPacket packet = null;
            try {
                packet = (JobTrackerPacket) inStream.readObject();
            } catch (IOException e) {
                System.out.println(identifier + "Remote thread is disconnected! Assuming remote client failed/exited.");
                handleDisconnect();
                return;
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            String result = "error";
            if (packet.jobType.equals("status")) {
                result = checkStatus(packet.data);
            } else if (packet.jobType.equals("submit")) {
                try {
                    result = submitJob(packet.data);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


        }
    }

    private String checkStatus(String hash) {
        Job j = new Job(hash,0);
        System.out.println("Getting status for location: "+j.resultPath());
        Job done = null;
        try {
            done = new Job( new String(zkc.getZooKeeper().getData(j.resultPath(), false, null)));
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Job is now " + done.jobStatus);
        System.out.println("C1 " + done.chunk1);
        System.out.println("C2 " + done.chunk2);
        System.out.println("C3 " + done.chunk3);

        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>REPORTING A STATUS OF "+done.jobStatus);
        JobTrackerPacket response = new JobTrackerPacket(done.jobStatus);
        response.data = done.result;

        try {
            this.outStream.writeObject(response);
        } catch (IOException e) {
            System.out.println(identifier + "Remote thread is disconnected! Assuming remote client failed/exited.");
            handleDisconnect();
        }

        return "not implemented";
    }

    private String submitJob(String hash) throws KeeperException, InterruptedException {
        System.out.println("SUBMITTING JOB WITH HASH "+hash);
        // Create three jobs and place them onto the job list.
        Job j1 = new Job(hash, 0);
        j1.chunkNumber = 1;
        Job j2 = new Job(hash, 0);
        j2.chunkNumber = 2;
        Job j3 = new Job(hash, 0);
        j3.chunkNumber = 3;

        // 0. SET UP PATHS

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

        Job j = new Job(hash, 0);
//        s = zkc.getZooKeeper().exists(j.resultPath(), false);
//        if (s != null) {
//            zkc.getZooKeeper().getData(j.resultPath(), false, s);
//            zkc.getZooKeeper().delete(j.resultPath(),s.getVersion());
//        }

        s = zkc.getZooKeeper().exists(j.resultPath(), false);
        if (s != null) {
            zkc.getZooKeeper().delete(j.resultPath(),s.getVersion());
        }
        zkc.getZooKeeper().create(j.resultPath(),j.serialize().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);


        // 2. enqueue it
        SyncPrimitive.Queue jobs = new SyncPrimitive.Queue("localhost", 2181, jobsPath);

        try {
            jobs.produce(j1.serialize(), hash+"-"+j1.chunkNumber);
            jobs.produce(j2.serialize(), hash+"-"+j2.chunkNumber);
            jobs.produce(j3.serialize(), hash+"-"+j3.chunkNumber);
        } catch (Exception e) {
            System.err.println("Test: error putting job up");
        }

        return "success";
    }

    private void handleDisconnect() {

    }
}



class JobTrackerPacket implements Serializable {
    String jobType = "invalid";
    String data = "invalid";

    public JobTrackerPacket() {

    }

    public JobTrackerPacket(String jobType) {
        this.jobType = jobType;
    }
}

