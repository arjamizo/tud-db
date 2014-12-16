
import java.util.ArrayList;
import java.util.Collection;
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
	int size = 1 << (8+1);
	ArrayList<LinkedList<Tuple> > buckets1 = new ArrayList(size);
	ArrayList<LinkedList<Tuple> > buckets2 = new ArrayList(size);
	for (int i = 0; i < size; i++) {
	    buckets1.add(new LinkedList());
	    buckets2.add(new LinkedList());
	}
	Tuple t;
	for (int i = 0; i < input1.size(); i++) {
	    t = input1.get(i);
	    int hash = hash(t.getID());
//	    System.out.printf("id=%d\thash=%d\n", t.getID(), hash);
	    buckets1.get(hash).add(t);
	}
	
	for (int i = 0; i < input2.size(); i++) {
	    t = input2.get(i);
	    int hash = hash(t.getID());
	    buckets2.get(hash).add(t);
	}
	
	LinkedList<Triple> ret = new LinkedList();
	for (int i = 0; i < buckets1.size(); i++) {
	    LinkedList<Tuple> inp1 = buckets1.get(i);
	    LinkedList<Tuple> inp2 = buckets2.get(i);
	    ret.addAll(new SortMergeJoin(0).join(inp1, inp2));
//	    ret.addAll(joinNormal(inp1, inp2));
	}
	return ret;
    }

    private List<Triple> joinNormal(LinkedList<Tuple> inp1, LinkedList<Tuple> inp2) {
	List<Triple> ret = new LinkedList();
	for (int i = 0; i < inp1.size(); i++) {
	    Tuple t1 = inp1.get(i);
	    	for (int j = 0; j < inp2.size(); j++) {
		Tuple t2 = inp2.get(j);
		if(t1.getID() == t2.getID()) {
		    ret.add(new Triple(t1.getID(), t1.getValue(), t2.getValue()));
		}
	    }
	}
	return ret;
    }

}
