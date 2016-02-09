DistributedMazewar
==================
To build

    make
    
To clean build

    make clean
    
To run nameserver

    chmod +x runserver.sh
    ./runserver.sh
    
To run client

    chmod +x run.sh
    ./run.sh
    
    
# Notes

This project uses ZooKeeper for:

* Atomic access of states
* Concurrent access of states
* Isolated access of states
* Durable and persistent changes of states

Moreover, we support:
* dynamic failure handling of server through primary/backup constructs
* dynamic failure handling of worker thorugh timeout pruning

The components in the system are:
* client: submits to jobtracker hashes to decrypt
* jobtracker: divides main job into subjobs and posts to zookeeper
* workers: pull subjobs and posts results
* fileserver: serves dictionary content to wokers

