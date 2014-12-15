
import java.util.ArrayList;
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
		    return key>>shift;
		}
	    });
	}
	System.out.println("sizes: "+input1.size()+" "+input2.size());
	int shift = (int)(Math.log(Math.sqrt(input1.size()*input2.size())));
	shift = Math.min(shift, funobjs.size());
	System.out.println("shift: "+shift);
	Objectifier fun = funobjs.get(shift);
	
	ArrayList<LinkedList<Tuple> > buckets = new ArrayList();
	buckets.ensureCapacity(1<<shift);
	System.out.println(buckets);
	return null;
    }

}
