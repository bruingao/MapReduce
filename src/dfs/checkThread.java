package dfs;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import Common.Scheduler;
import Common.Util;

public class checkThread implements Runnable{

	private String dnode;
	private int dnodeport;
	private String serviceName;
	private String registryAddress;
	private int registryPort;
	
	private OP op;

	private String filename;
	
	private int chunknumber;

	private String[] nodes;

	public enum OP  {WRITE, STATUS, DELETE};
	
	public checkThread(String dn, int dnp, String sname, String ra, int rp)
	{
		dnode = dn;
		dnodeport = dnp;
		serviceName = sname;
		registryAddress = ra;
		registryPort = rp;
	}
	
	public void setChunknumber(int chunknumber) {
		this.chunknumber = chunknumber;
	}
	
	public void setFilename(String s) {
		filename = s;
	}
	
	public void setOp(OP op) {
		this.op = op;
	}
	
	public void setNodes(String[] nodes) {
		this.nodes = nodes;
	}
	
	@Override
	public void run() {
		switch(op) {
			case STATUS:
				checkStatus();
				break;
			case WRITE:
				write();
				break;
			case DELETE:
				delete();
				break;
			default:
				break;
		}
	}
	
	private void checkStatus() {
		Registry registry;
		boolean status = false;
		try {
			registry = LocateRegistry.getRegistry(registryAddress, registryPort);
			DataNodeI datanode = (DataNodeI) registry.lookup("rmi://"+dnode+dnodeport+"/"+serviceName);
			status = datanode.heartBeat();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			Scheduler.getStatus().put(dnode, status);
		}
	}
	
	private void write() {
		Registry registry;
		try {
			registry = LocateRegistry.getRegistry(registryAddress, registryPort);
			DataNodeI datanode = (DataNodeI) registry.lookup("rmi://"+dnode+dnodeport+"/"+serviceName);
			boolean res = datanode.replication(filename + chunknumber, nodes);
			
			if (res) {
				Scheduler.replication(filename, chunknumber, dnode);
				/* checkpoint */
				Util.writeObject(NameNode.nameNodePath+"files", Scheduler.getFiles());
				Util.writeObject(NameNode.nameNodePath+"nodeToReplicas", Scheduler.getNodeToReplicas());
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	private void delete() {
		Registry registry;
		try {
			registry = LocateRegistry.getRegistry(registryAddress, registryPort);
			DataNodeI datanode = (DataNodeI) registry.lookup("rmi://"+dnode+dnodeport+"/"+serviceName);
			datanode.removeFile(filename + chunknumber);
			
			Scheduler.removeReplica(filename, chunknumber, dnode);
			/* checkpoint */
			Util.writeObject(NameNode.nameNodePath+"files", Scheduler.getFiles());
			Util.writeObject(NameNode.nameNodePath+"nodeToReplicas", Scheduler.getNodeToReplicas());

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

}
