package rmi;

import java.io.*;


public class FilePacket implements Serializable {

	private String name;

  private byte[] data;

  public FilePacket( String name ){
  	this.name = name;
  }

  public String getName(){
  	return name;
  }

  public void readIn(){
  	try{
    	File file = new File( name );
     	data = new byte[ (int)(file.length()) ];
         (new FileInputStream( file )).read( data );
    }catch( Exception e ){
    	e.printStackTrace();
   	}
  }

  public void writeTo( OutputStream out  ){
  	try{
    	out.write( data );
  	}catch( Exception e ){
   		e.printStackTrace();
   	}
  }
}
