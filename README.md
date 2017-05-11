# socket_server_example #
This is a simple tcp based cache implementation. It persists the cache to disk on exit but all new data will be lost on crrash. Previously stored data is loaded on starup. 

An example client that illustrates how to communicate with the server can be found in `client.py` or in `/src/test/scala/lolvang/sockserver/Client.scala` 

### Build instructions ###
To build the project you must have maven and a java 8 jdk in your path.
Once these are installed build and test by running `mvn package` to run the integration tests for the server run `mvn verify`

### starting the server ###
Once built the server can be started using the following command `java -jar target/sockserver-1.0-fat.jar`
