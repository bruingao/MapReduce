package mapred;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class JobTacker extends UnicastRemoteObject implements JobTrackerI {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5442874947046318711L;

	protected JobTacker() throws RemoteException {
		super();
	}

}
