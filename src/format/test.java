import java.io.*;
import java.util.*;
import java.lang.Math;

public class test {

    public static void main(String[] args) throws Exception {
        /*
        inputFormatLine ifp= new inputFormatLine("a b\nc d");
        List<inputFormatAbs.kvPair> kvPairs=ifp.getkvPairs();
        System.out.println(kvPairs.get(0).key);
        System.out.println(kvPairs.get(0).value);
        System.out.println(kvPairs.get(1).key);
        System.out.println(kvPairs.get(1).value);
        System.out.println(kvPairs.get(2).key);
        System.out.println(kvPairs.get(2).value);
        System.out.println(kvPairs.get(3).key);
        System.out.println(kvPairs.get(3).value);
        */
        Collector c=new Collector();
        inputFormatPair ifp1= new inputFormatPair("009 b\n0402 d\n1 n\n3 d\n2 5\n10 e");
        inputFormatPair ifp2= new inputFormatPair("0 b\n07 d\n14 n\n39 d\n201 5\n10 e");
        List<inputFormatAbs.kvPair> ps1=ifp1.getkvPairs();
        List<inputFormatAbs.kvPair> ps2=ifp2.getkvPairs();

        c.addkvPairs(ps1);
        c.addkvPairs(ps2);
        c.sortIntKey();
        /*
        for (int i=0;i<12;i++){
            System.out.println(c.collection.get(i).key);
            System.out.println(c.collection.get(i).value);
        }
        */
        
        
        
        String[] result=Partitioner.partition(c.collection,c.uniqueKeys,3);
        
        //outputFormat oformat=new outputFormat(c.collection);
        System.out.println(result[0]);
        System.out.println(result[1]);
        System.out.println(result[2]);
        
    }

}
