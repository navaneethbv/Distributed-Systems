import java.util.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by Navaneeth Rao
 */
public class Job {

    public Job(String hash, long timestamp) {
        this.hash = hash;
        this.timestamp = timestamp;
    }

    public Job(String serialized) {
        List<String> jobList = Arrays.asList(serialized.split(";"));
        assert (jobList.size() == 8);
        chunkNumber = Integer.parseInt(jobList.get(0));
        hash = jobList.get(1);
        result = jobList.get(2);
        chunk1 = jobList.get(3);
        chunk2 = jobList.get(4);
        chunk3 = jobList.get(5);
        jobStatus = jobList.get(6);
        timestamp = Long.parseLong(jobList.get(7));

    }

    public String serialize() {
        return chunkNumber + ";" + hash + ";" + result + ";" + chunk1 + ";" + chunk2 + ";" + chunk3 + ";" + jobStatus + ";" + timestamp;
    }

    public String toString() {
        return "CN:"+chunkNumber + ";HASH:" + hash + ";RESULT:" + result + ";C1:" + chunk1 + ";C2:" + chunk2 + ";C3:" + chunk3 + ";JS:" + jobStatus + ";TS:" + timestamp;
    }

    public String workingPath() {
        return "/working/" + hash + "-" + chunkNumber;
    }

    public String resultPath() {
        return "/results/" + hash;
    }

    public String pendingPath() {
        return "/pending/" + hash + "-" + chunkNumber;
    }

    int chunkNumber; // where does the dictionary start
    String hash; // what hash are we decoding?
    String result; // what's the answer for this hash?
    String chunk1 = "pending";
    String chunk2 = "pending";
    String chunk3 = "pending";
    String jobStatus = "pending"; // what's the status of this job?
    long timestamp;

    public static String getHash(String word) {
        String hash = null;
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            BigInteger hashint = new BigInteger(1, md5.digest(word.getBytes()));
            hash = hashint.toString(16);
            while (hash.length() < 32) hash = "0" + hash;
        } catch (NoSuchAlgorithmException nsae) {
            // ignore
        }
        return hash;
    }

}
