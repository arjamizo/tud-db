
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

interface Objectifier {
    int execute(int params); 
}

public class HashJoin implements Join {

    public String getName() {
	return "Hash Join";
    }

	int cores = 0;
	public HashJoin() {
		this.cores = -1;
	}
	
	public HashJoin(int cores) {
		this.cores = cores;
	}
    
    public static int hash(int id) {
	return id >>> 8;
    }

    public List<Triple> join(final List<Tuple> input1, final List<Tuple> input2) {
	int size = 1 << (8+1);
	final ArrayList<LinkedList<Tuple> > buckets1 = new ArrayList(size);
	final ArrayList<LinkedList<Tuple> > buckets2 = new ArrayList(size);
	for (int i = 0; i < size; i++) {
	    buckets1.add(new LinkedList());
	    buckets2.add(new LinkedList());
	}
	
	
	List<Runnable> tasks = Collections.synchronizedList(new LinkedList());
	
	new Runnable() {
		public void run() {
			for (int i = 0; i < input1.size(); i++) {
				Tuple t = input1.get(i);
				int hash = hash(t.getID());
				//System.out.printf("id=%d\thash=%d\n", t.getID(), hash);
				buckets1.get(hash).add(t);
			}
		}
	}.run();
	new Runnable() {
		public void run() {
			for (int i = 0; i < input2.size(); i++) {
				Tuple t = input2.get(i);
				int hash = hash(t.getID());
				buckets2.get(hash).add(t);
			}
		}
	}.run();
	
	int maxid = 0; 
	if(this.cores == -1) {
		maxid = input1.size()*input2.size()>5e10 ? Runtime.getRuntime().availableProcessors() : 0;
//		maxid = Runtime.getRuntime().availableProcessors();
	}
	Thread[] threads = new Thread[maxid];
	
	for (int i = 0; i < maxid; i++) {
		threads[i]=new Thread(new Runnable() {
			List<Runnable> tasks;
			public Runnable init(List<Runnable> tasks) {
				this.tasks=tasks;
				return this;
			}
			public void run() {
				while(this.tasks.size()>0) {
					Runnable r = null;
					synchronized(this.tasks) {
						if(this.tasks.size()!=0) {
							r = tasks.remove(0);
						}
					}
					if(r!=null) r.run();
				}
			}
		}.init(tasks));
	}
	
	final List<Triple> ret = Collections.synchronizedList(new LinkedList());
	
	for (int i = 0; i < buckets1.size(); i++) {
		tasks.add(new Runnable() {
			int i;
			public Runnable init(int i) {
				this.i=i;
				return this;
			}
			public void run() {
				LinkedList<Tuple> inp1 = buckets1.get(i);
				LinkedList<Tuple> inp2 = buckets2.get(i);
				List re = new SortMergeJoin(0).join(inp1, inp2);
				ret.addAll(re);
			}
		}.init(i));
		//ret.addAll(joinNormal(inp1, inp2));
	}
	
	for (int i = 0; i < maxid; i++)
		threads[i].start();

	for (int i = 0; i < maxid; i++) {
		try {
			threads[i].join();
		} catch (InterruptedException ex) {
			Logger.getLogger(HashJoin.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	if(maxid==0) for (Runnable runnable : tasks) {
			runnable.run();
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
