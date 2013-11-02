javac rmi/*.java
rmic rmi.RemoteReceiver

on receiver(DataNode):
rmiregistry
java -Djava.security.policy=all.policy rmi.RemoteReceiver /home/ubuntu/15640proj3/RMIstream/tmp/ localhost

on sender(DataNode):
java -Djava.security.policy=all.policy rmi.FileSender /home/ubuntu/HelloWorld.java <receiverHostname>

on rebinder(NameNode):
...
