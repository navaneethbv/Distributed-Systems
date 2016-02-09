build:
	javac -cp zookeeper-3.3.2.jar:log4j-1.2.15.jar:. ZkConnector.java Fileserver.java SyncPrimitive.java Job.java Worker.java DummyJobSubmitter.java JobTracker.java Client.java

fileserver:
	java -cp zookeeper-3.3.2.jar:log4j-1.2.15.jar:. Fileserver localhost:2181 lowercase.rand

worker:
	java -cp zookeeper-3.3.2.jar:log4j-1.2.15.jar:. Worker localhost 2181 slimShady

syncprimitive:
	java -cp zookeeper-3.3.2.jar:log4j-1.2.15.jar:. SyncPrimitive p

dummyjob:
	java -cp zookeeper-3.3.2.jar:log4j-1.2.15.jar:. DummyJobSubmitter

jobtracker:
	java -cp zookeeper-3.3.2.jar:log4j-1.2.15.jar:. JobTracker localhost:2181

client:
	java -cp zookeeper-3.3.2.jar:log4j-1.2.15.jar:. Client

clean:
	rm -f *.class *~