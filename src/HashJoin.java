
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

    public List<Triple> join(final List<Tuple> input1, final List<Tuple> input2) {
	ArrayList<Objectifier> funobjs = new java.util.ArrayList();
	for (int i = 0; i < 16; i++) {
	    funobjs.add(new Objectifier() {
		int shift;
		public Objectifier init(int shift) {
		    this.shift = shift;
		    return this;
		}
		public int execute(int key) {
		    return key>>>shift;
		}
	    }.init(i));
	}
	System.out.println("sizes: "+input1.size()+" "+input2.size());
	int shift = (int)(Math.log10(Math.sqrt(input1.size()*input2.size())));
	
	Comparator <Tuple> cmp = new Comparator<Tuple>() {
		public int compare(Tuple o1, Tuple o2) {
			return o1.getID()-o2.getID();
		}
	};
	shift = (int)Math.log((Collections.max(input1, cmp).getID()-Collections.min(input1, cmp).getID()));
//	shift *= Math.log10(input1.size()*input2.size());
	shift = Math.min(shift, funobjs.size()-1);
	System.out.println("shift: "+shift);
	
	Objectifier fun = funobjs.get(shift);
	
	int size = 1<<shift;
	System.out.println("intialCapacity: "+size);
	ArrayList<LinkedList<Tuple> > buckets = new ArrayList(size);
	for (int i = 0; i < size; i++) {
	    buckets.add(new LinkedList());
	}
	
	for (int i = 0; i < input1.size(); i++) {
	    Tuple t = input1.get(i);
	    int hash = fun.execute(t.getID());
	    System.out.printf("id=%d\thash=%d\n", t.getID(), hash);
	}
	return null;
    }

}
