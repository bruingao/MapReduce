package format;

import java.util.*;

public abstract class inputFormatAbs{
    
    public class kvPair {
        public String key;
        public String value;

        public kvPair(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }
    
    public List<kvPair> kvPairs = new ArrayList<kvPair>();

    public String content;
    
    public abstract List<kvPair> getkvPairs();
}
