package Common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.lang.reflect.Field;
import java.util.ArrayList;

/**
 * Util is a utility class for our program. It compute the hash code of a givin method.
 * 
 * @author      Rui Zhang
 * @author      Jing Gao
 * @version     1.0, 10/8/2013
 * @since       1.0
 */
public final class Util {

    /** 
     * constructor of Util class
     *
     * @since           1.0
     */
	private Util() {}
	
	/** 
     * write specific content into a file called filename 
     *
     * @param content   the content we want to write into a file
     * @param filename  the naem of the file that we want to write 
     * @since           1.0
     */
	public static void writeToFile(String content, String filename) {
		File newfile = new File(filename);
		if(!newfile.exists()) {
			try {
				newfile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} 
		FileOutputStream out = null;
		try {
			out = new FileOutputStream(newfile);
			out.write(content.getBytes(), 0, content.getBytes().length);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/** 
     * read content from a file called filename 
     *
     * @param filename  the naem of the file that we want to read
     * @return			the content of the file 
     * @since           1.0
     */
	public static byte[] readFromFile(String filename) {
		File newfile = new File(filename);
		if(!newfile.exists()) {
			System.out.println("File " +filename+" not exist!");
			return null;
		} 
		FileInputStream in = null;
		byte[] content = new byte[(int) newfile.length()];
		try {
			in = new FileInputStream(newfile);
			in.read(content);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				in.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return content;
	}
	
	public static void writeBinaryToFile(byte[] content, String filename) {
		File newfile = new File(filename);
		if(!newfile.exists()) {
			try {
				newfile.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} 
		FileOutputStream out = null;
		try {
			out = new FileOutputStream(newfile);
			out.write(content);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	
	public static String[] parseLine(String line, String del) {
		String res[] = new String[2];
		int pos = line.lastIndexOf(del);
		
		if (pos == -1) {
			return null;
		}
		
		res[0] = line.substring(0, pos);
		res[1] = line.substring(pos+1, line.length());
		
		return res;
		
	}
	
	
	public static void readConfigurationFile(String filename, Object obj) {
		String content = null;
		try {
			content = new String(Util.readFromFile(filename), "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String lines[] = content.split("\n");
		for(String line : lines) {
			String temp[] = Util.parseLine(line, "=");
			
			try {
				Field field = obj.getClass().getDeclaredField(temp[0]);
				field.setAccessible(true);
				if(field.getType().isPrimitive()){
					field.setInt(obj, Integer.parseInt(temp[1]));
				} else if (field.getType().equals(String.class)) {
					field.set(obj, temp[1]);
				} else if (field.getType().equals(Integer.class)) {
					field.set(obj, Integer.parseInt(temp[1]));
				}
			} catch (NoSuchFieldException e){
				
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void writeObject(String filename, Object obj) {
		File newfile = new File(filename);
		if(!newfile.exists()) {
			try {
				newfile.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} 
		FileOutputStream out = null;
		ObjectOutputStream oout = null;
		try {
			out = new FileOutputStream(newfile);
			oout = new ObjectOutputStream(out);
			oout.writeObject(obj);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				out.close();
				oout.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static Object readObject(String filename) {
		File newfile = new File(filename);
		if(!newfile.exists()) {
			System.out.println("File "+filename+" not exist!");
			return null;
		} 
		FileInputStream in = null;
		Object content = null;
		ObjectInputStream iin = null;
		try {
			in = new FileInputStream(newfile);
			iin = new ObjectInputStream(in);
			content = iin.readObject();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			try {
				in.close();
				iin.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return content;
	}
	
	public static String convertStreamToStr(InputStream is) throws IOException {
		 
		if (is != null) {
			Writer writer = new StringWriter();
			 
			char[] buffer = new char[1024];
			try {
			Reader reader = new BufferedReader(new InputStreamReader(is,
					"UTF-8"));
				int n;
				while ((n = reader.read(buffer)) != -1) {
					writer.write(buffer, 0, n);
				}
			} finally {
				is.close();
			}
			return writer.toString();
		} else {
			return "";
		}
	}
		 
	public static int decideChunkNumber (int filesize, int chunksize, ArrayList<Integer> range, byte[] content) {
		
		int chunknumber = 0;
		int basis=chunksize;
		
		range.add(0);
		
		while (basis<filesize-1){
		    if(content[basis-1]=='\n'){
		    }else{
		        while(basis < filesize && content[basis-1]!='\n'){
		            basis++;
		        }
		    }
		    range.add(basis);
		    chunknumber++;
		    basis+=chunksize;
		}
		
		if(range.get(range.size()-1) < filesize) {
			range.add(filesize);
			chunknumber++;
		}
		
		return chunknumber;
		
	}
		
}