
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

interface Objectifier {
    int execute(int params); 
}

public class HashJoin implements Join {

    public String getName() {
	return "Hash Join";
    }
    
    public static int hash(int id) {
	return id >>> 8;
    }

    public List<Triple> join(final List<Tuple> input1, final List<Tuple> input2) {
	int size = 1 << 8;
	ArrayList<LinkedList<Tuple> > buckets = new ArrayList(size);
	for (int i = 0; i < size; i++) {
	    buckets.add(new LinkedList());
	}
	
	for (int i = 0; i < input1.size(); i++) {
	    Tuple t = input1.get(i);
	    int hash = hash(t.getID());
	    System.out.printf("id=%d\thash=%d\n", t.getID(), hash);
	}
	return null;
    }

}
