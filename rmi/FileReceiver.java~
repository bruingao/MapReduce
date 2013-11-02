package rmi;

import java.rmi.*;

public interface FileReceiver extends Remote {

   public void setDirectory( String directory ) throws RemoteException;
   
   public void receiveFile( FilePacket packet ) throws RemoteException;
   
   public void proxyRebind(FileReceiver receiver) throws RemoteException;
}
