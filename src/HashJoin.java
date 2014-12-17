import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class HashJoin implements Join {

    public String getName() {
		return "Hash Join";
    }
	
	static {
		try {
			JoinTesting.testInterface(new HashJoin());
		} catch (Throwable e) {
			System.err.println("Cannot run tests for HashJoin");
		}
	}

    public List<Triple> join(final List<Tuple> input1, final List<Tuple> input2) {
		int initialCapacity = (int)1e6;
		Map<Integer, List<Tuple>> buckets1 = new java.util.HashMap(initialCapacity), buckets2 = new java.util.HashMap(initialCapacity);
		putIntoBuckets(input1, buckets1);
		putIntoBuckets(input2, buckets2);
		
		return performZipCrossJoin(buckets1, buckets2);
	}

	private void putIntoBuckets(final List<Tuple> input1, Map<Integer, List<Tuple>> buckets1) {
		for (int i = 0; i < input1.size(); i++) {
			Tuple t = input1.get(i);
			List<Tuple> l = buckets1.get(t.getID());
			if(l == null) {
				buckets1.put(t.getID(), l = new LinkedList());
			}
			l.add(t);
		}
	}

	/**
	 * Iterates through all lists contained in maps passed in arguments 
	 * and combines results. Let's finish this small-talkie and let's get to the point. 
	 * Consider following example: 
	 * 
	 * buckets1 :
	 * {1: a b,
	 *  2: c d}
	 * buckets2: 
	 * {1: e,
	 *  2: h u j,
	 *  3: i}
	 * 
	 * This function returns 
	 * {(1 a e), (1 b e)
	 *  (2 c h), (2 c u) (2 c j) (2 d h) (2 d u) (2 d j)
	 * }
	 * Note the fact that element with id=3 has null on one of "sides".
	 * 
	 * Name of this function is a wordplay of: 
	 *	- zip - operation of combining of two lists
	 *  - cross-product - way of finding all possible pairs among two sets
	 * Author finds naming function this way very funny. Author silently suggests you doing the same.
	 */
	private List<Triple> performZipCrossJoin(Map<Integer, List<Tuple>> buckets1, Map<Integer, List<Tuple>> buckets2) {
		List<Triple> ret = new LinkedList();
		for (Integer key : buckets1.keySet()) {
			List<Tuple> l1 = buckets1.get(key);
			List<Tuple> l2 = buckets2.get(key);
			if(l2 == null) continue;
			for (Tuple t1 : l1) {
				for (Tuple t2 : l2) {
					ret.add(new Triple(key, t1.getValue(), t2.getValue()));
				}
			}
		}
		
		/* There might be some keys, which are present 
		   in buckets2, but not in buckets1 */
		java.util.Set<Integer> diff = buckets2.keySet();
		diff.removeAll(buckets1.keySet());
		for (Integer key : diff ) {
			if(buckets1.containsKey(key)) continue; //it was already processed
			List<Tuple> l1 = buckets2.get(key);
			List<Tuple> l2 = buckets1.get(key);
			if(l2 == null) continue;
			for (Tuple t1 : l1) {
				for (Tuple t2 : l2) {
					ret.add(new Triple(key, t1.getValue(), t2.getValue()));
				}
			}
		}
		return ret;
	}

}
