package rmi;

import java.rmi.*;
import java.rmi.server.*;
import java.io.*;
import java.util.*;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;

public class ProxyRebinder extends UnicastRemoteObject implements FileReceiver {

  public ProxyRebinder() throws RemoteException {
  	super();
  }

  public void setDirectory( String directory ) throws RemoteException{
  }

  public void receiveFile( FilePacket packet ) throws RemoteException {

  }

  public void proxyRebind(FileReceiver receiver) throws RemoteException{
    
  }

  public static void main( String args[] ) {

  }

}
