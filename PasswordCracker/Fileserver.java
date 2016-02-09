import com.sun.tools.doclets.formats.html.resources.standard;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.net.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Fileserver {

    String myPath = "/fileserver";
    ZkConnector zkc;
    Watcher watcher;

    private static String dictionaryPath;
    private static String zookeeperAddress;
    private ServerSocket serverSocket;
    private final int serverSocketPort = 8888;
    private static String myAddress;

    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Fileserver zkServer:clientPort dictionary");
            return;
        }

        zookeeperAddress = args[0];
        dictionaryPath = args[1];

        try {
            myAddress = java.net.InetAddress.getLocalHost().getCanonicalHostName();
        } catch (Exception e) { System.err.println("Fileserver: Cannot obtain own hostname"); }

        Fileserver t = new Fileserver(zookeeperAddress);

        System.out.println("Sleeping...");
        try {
            Thread.sleep(5000);
        } catch (Exception e) {}

        t.checkpath();

        System.out.println("Sleeping...");
        while (true) {
            try{ Thread.sleep(5000); } catch (Exception e) {}
        }
    }

    public Fileserver(String hosts) {
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
            Code ret = zkc.create(
                    myPath,         // Path of znode
                    myAddress,           // Data is my address.
                    CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
            );
            if (ret == Code.OK) System.out.println("I am the primary fileserver!");

            // got the job, now open up and listen
            try {
                serverSocket = new ServerSocket(serverSocketPort);
            } catch (IOException e) {
                System.err.println("Fileserver failed to establish server socket on " + serverSocketPort);
                System.exit(1);
            }

            // keep listening
            while (true) {
                System.out.println("Fileserver: I'm listening and listening and listening at " + serverSocket.getInetAddress().getCanonicalHostName());
                try {
                    Socket sock = serverSocket.accept();
                    System.out.println("A sucker connected from " + sock.getInetAddress().getCanonicalHostName());
                    FileserverHandler handler = new FileserverHandler(sock, dictionaryPath);
                    new Thread(handler).start();
                } catch (IOException e) {
                    System.err.println("Fileserver failed to accept socket connection");
                }
            }
        }
    }

    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(myPath)) {
            if (type == EventType.NodeDeleted) {
                System.out.println(myPath + " deleted! Other guy is down!");
                checkpath(); // try to become the boss
            }
            if (type == EventType.NodeCreated) {
                System.out.println(myPath + " Primary fileserver created!");
                try{ Thread.sleep(5000); } catch (Exception e) {}
                checkpath(); // re-enable the watch
            }
        }
    }

}


class FileserverHandler implements Runnable {

    private Socket mySock;
    private String dictionaryPath;
    private ArrayList<String> lines;

    public ObjectInputStream inStream;
    public ObjectOutputStream outStream;

    static int instanceCounter = 0;

    private String identifier;

    public FileserverHandler(Socket sock, String dictionaryPath) throws FileNotFoundException {
        this.mySock = sock;
        this.dictionaryPath = dictionaryPath;
        System.out.println("new FSHandler: I be readin' 'em files...");
        Scanner s = new Scanner(new File(dictionaryPath));
        lines = new ArrayList<String>();
        while (s.hasNext()){
            lines.add(s.next());
        }
        s.close();
        identifier = "Fileserver Handler " + instanceCounter + ": ";
        instanceCounter++;
        System.out.println(identifier + " I done readin' 'em files... " + lines.size() + " lines");
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
            FileserverPacket packet = null;
            try {
                packet = (FileserverPacket) inStream.readObject();
            } catch (IOException e) {
                System.out.println(identifier + "Remote thread is disconnected! Assuming remote client failed/exited.");
                handleDisconnect();
                return;
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            FileserverPacket response;
            if (packet.chunkNumber >= 1 && packet.chunkNumber <= 3) {
                int startLine = 100000*(packet.chunkNumber - 1);
                int endLine = 100000*(packet.chunkNumber)-1;
                if(endLine > this.lines.size()){
                    endLine = this.lines.size()-1;
                }

                List<String> requestedLines = this.lines.subList(startLine, endLine);
                response = new FileserverPacket(packet.chunkNumber, requestedLines);
                response.ok = true;
            } else {
                response = new FileserverPacket(-1, new ArrayList<String>());
            }

            try {
                this.outStream.writeObject(response);
            } catch (IOException e) {
                System.out.println(identifier + "Remote thread is disconnected! Assuming remote client failed/exited.");
                handleDisconnect();
                return;
            }
        }
    }

    private void handleDisconnect() {

    }
}



class FileserverPacket implements Serializable {
    int chunkNumber;
    List<String> lines;
    boolean ok = false;

    public FileserverPacket() {

    }

    public FileserverPacket(int chunkNumber, List<String> lines) {
        this.chunkNumber = chunkNumber;
        this.lines = new ArrayList<String>(lines.size());
        for (String s : lines) {
            this.lines.add(s);
        }
    }
}