package mapred;

import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class JobTacker extends UnicastRemoteObject implements JobTrackerI {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5442874947046318711L;
	
	private static String confPath = "src/conf/mapred.conf";
	
	private static String slavePath = "src/conf/slaves";
	
	/* max number of mappers run on one machine */
	private static Integer maxMappers;
	
	/* max number of reducers run on one machine */
	private static Integer maxReducers;
	
	/* job tracker's host address */
	private static String jobHostname;
	
	/* job tracker's port number */
	private static Integer jobPort;
	
	/* task tracker's port number */
	private static Integer taskPort;
	
	/* registry port number */
	private static Integer registryPort;
	
	/* registry */
	Registry registry;
	
	
	protected JobTacker() throws RemoteException {
		super();
	}

}
