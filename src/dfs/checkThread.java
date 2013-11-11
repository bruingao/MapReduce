package dfs;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import Common.dfsScheduler;
import Common.Util;

public class checkThread implements Runnable{

	private String dnode;
	//private int dnodeport;
	private int registryPort;
	private String serviceName;
	
	private OP op;

	private String filename;
	
	private int chunknumber;

	private String[] nodes;

	public enum OP  {WRITE, STATUS, DELETE};
	
	public checkThread(String dn, int rp, String sname)
	{
		dnode = dn;
		//dnodeport = dnp;
		registryPort=rp;
		serviceName = sname;
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
		boolean status = false;
		try {
		    Registry dnRegistry=LocateRegistry.getRegistry(dnode,registryPort);
			//DataNodeI datanode = (DataNodeI) NameNode.registry.lookup(dnode+"/"+serviceName);
			DataNodeI datanode = (DataNodeI) dnRegistry.lookup(serviceName);
			status = datanode.heartBeat();
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			if (e instanceof java.rmi.NotBoundException) {
				System.out.println("Datanode "+dnode+" not startup.");
			}
			
			else if(e instanceof java.rmi.ConnectException){
			    
			}
			
			else {
				e.printStackTrace();
			}
		} finally {
			dfsScheduler.getStatus().put(dnode, status);
		}
	}
	
	private void write() {
		try {
		    Registry dnRegistry=LocateRegistry.getRegistry(dnode,registryPort);
			//DataNodeI datanode = (DataNodeI) NameNode.registry.lookup(dnode+"/"+serviceName);
			DataNodeI datanode = (DataNodeI) dnRegistry.lookup(serviceName);
			boolean res = datanode.replication(filename + chunknumber, nodes);
			
			if (res) {
				dfsScheduler.replication(filename, chunknumber, dnode);
				/* checkpoint */
				Util.writeObject(NameNode.nameNodePath+"files", dfsScheduler.getFiles());
				Util.writeObject(NameNode.nameNodePath+"nodeToReplicas", dfsScheduler.getNodeToReplicas());
			}
			
		} catch (Exception e) {
			if (e instanceof java.rmi.NotBoundException) {
				System.out.println("Datanode "+dnode+" not startup.");
			}
			
			else if(e instanceof java.rmi.ConnectException){
			    
			}
			
			else {
				e.printStackTrace();
			}
		} 
	}
	
	private void delete() {
		try {
		    Registry dnRegistry=LocateRegistry.getRegistry(dnode,registryPort);
			DataNodeI datanode = (DataNodeI) dnRegistry.lookup(serviceName);
			datanode.removeFile(filename + chunknumber);
			
			dfsScheduler.removeReplica(filename, chunknumber, dnode);
			/* checkpoint */
			Util.writeObject(NameNode.nameNodePath+"files", dfsScheduler.getFiles());
			Util.writeObject(NameNode.nameNodePath+"nodeToReplicas", dfsScheduler.getNodeToReplicas());

		} catch (Exception e) {
			if (e instanceof java.rmi.NotBoundException) {
				System.out.println("Datanode "+dnode+" not startup.");
			}
			
			else if(e instanceof java.rmi.ConnectException){
			    
			}
			
			else {
				e.printStackTrace();
			}
		} 
	}

}
