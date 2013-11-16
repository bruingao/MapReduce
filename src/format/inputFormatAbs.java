package format;

import java.util.*;

import Common.Pair;

public abstract class inputFormatAbs{

    
    public List<Pair> kvPairs = new ArrayList<Pair>();

    public String content;
    
    public abstract List<Pair> getkvPairs();
}
