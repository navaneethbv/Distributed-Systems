import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.Leader;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created by Navaneeth Rao
 */
public class Client {
    ZkConnector zkc;
    ZooKeeper zk;

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

    public static void main(String args[]) throws KeeperException, InterruptedException, IOException, ClassNotFoundException {
        if(args.length != 3){
            System.out.println("Error: 3 args not given");
            System.exit(1);
        }
        String ZkAddress = args[0];
        String hash = args[1];
        String operation = args[2];

        Client c = new Client(ZkAddress, hash, operation);
    }

    public Client(String ZkAddress, String hash, String operation) throws KeeperException, InterruptedException, IOException, ClassNotFoundException {
        if(operation.equals("submit") || operation.equals("status")){
            // Valid command
        }else{
            System.out.println("Error: invalid operation");
            System.exit(1);
        }

        // Connect to the ZooKeeper server
        zkc = new ZkConnector();
        try {
            zkc.connect(ZkAddress);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        // Get the primary Job Tracker
        zk = zkc.getZooKeeper();

        String jobtrackerHostname = new String(zk.getData("/jobtracker", false, null));
        Socket jobtrackerSocket = new Socket(jobtrackerHostname, 8889);
        ObjectInputStream inputStream = new ObjectInputStream(jobtrackerSocket.getInputStream());
        ObjectOutputStream outputStream = new ObjectOutputStream(jobtrackerSocket.getOutputStream());

        JobTrackerPacket request;
        request = new JobTrackerPacket(operation);
        request.data = hash;
        outputStream.writeObject(request);

        if(operation.equals("status")) {

            //Wait for response
            JobTrackerPacket response = (JobTrackerPacket) inputStream.readObject();
            if (response.jobType.equals("pending") || response.jobType.equals("working") || response.jobType.equals("pass") || response.jobType.equals("fail")) {
                System.out.print("Status: ");
                if (response.jobType.equals("pending")) {
                    System.out.println(ANSI_YELLOW + "Pending");
                }
                if (response.jobType.equals("working")) {
                    System.out.println(ANSI_BLUE + "Working");
                }
                if (response.jobType.equals("pass")) {
                    System.out.println(ANSI_GREEN + "Pass");
                    System.out.println(ANSI_RESET + "Found Password: " + response.data);
                }
                if (response.jobType.equals("fail")) {
                    System.out.println(ANSI_RED + "Fail");
                }
            }
        }
    }
}
