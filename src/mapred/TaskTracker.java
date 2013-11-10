package mapred;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class TaskTracker extends UnicastRemoteObject implements TaskTrackerI{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6393332207622467820L;

	protected TaskTracker() throws RemoteException {
		super();
	}

}
