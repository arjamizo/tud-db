
import java.util.ArrayList;
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
	return null;
    }

}
