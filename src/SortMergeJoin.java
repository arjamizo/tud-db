import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

class PerfTest {
	java.util.ArrayList<Runnable> tests = new java.util.ArrayList();
	java.util.ArrayList<Long> times = new java.util.ArrayList();
	java.util.ArrayList<String> names = new java.util.ArrayList();
	public PerfTest addTest(String name, Runnable test) {
		tests.add(test);
		names.add(name);
		return this;
	}
	public void perform() {
		for (Runnable test : tests) {
			long start = System.currentTimeMillis();
			test.run();
			long end = System.currentTimeMillis();
			
			times.add(end-start);
		}
		int idx = times.indexOf(java.util.Collections.min(times));
		System.out.printf("Names: \n\t%s\nTims:\n\t%s\n", names, times);
		System.out.printf("The fastest is: \n\t%s. With time of %d[ms]\n\n", names.get(idx), times.get(idx));
	}
}

/**
 * new SortMergeJoin(0) - run without threads
 * new SortMergeJoin(N) - N>0 - run on N threads
 * new SortMergeJoin(-1)- automatically run on all available threads if more than cross-product possibilities. 
 * @author azochniak
 */
public class SortMergeJoin implements Join{
	static Comparator<Tuple> cmp;
	static {
		cmp = new Comparator<Tuple>() {
			public int compare(Tuple o1, Tuple o2) {
				return o1.getID()-o2.getID();
			}
		};
		/*
		new PerfTest().addTest("finishing iteration by learning max size", new Runnable() {

			List<String> list = Arrays.asList((new String(new char[10000]).replace("\0", "a")).split("a"));
			String str = "";
			
			public void run() {
				for (int i = 0; i < list.size(); i++) {
					str += list.get(i);
				}
				System.out.println(str.length());
			}
		}).addTest("finishing iteration by occurence of an exception", new Runnable() {

			List<String> list = Arrays.asList((new String(new char[10000]).replace("\0", "a")).split("a"));
			String str = "";
			
			public void run() {
				try {
					for (int i = 0; ; i++) {
						str += list.get(i);
					}
				} catch (ArrayIndexOutOfBoundsException e) {
					System.out.println(str.length());
				}
			}
		}).perform();
		*/
		System.out.println("<testing-framework>");
		/*
		testInterface(new SortMergeJoin());
		*/
		System.out.println("</testing-framework>");
	}

	public String getName() {
		return "Sort Merge Join";
	}
	
	int cores = 0;
	public SortMergeJoin(int cores) {
	    this.cores = cores;
	}
	
	/**
	 * Do not use multiple cores by default.
	 */
	public SortMergeJoin() {
		this.cores = 0;
	}

	public static <T> int upper_bound(T[] arr, T key, Comparator<T> c, int from, int to) {
	    int len = to;
	    if(to==-1) len = arr.length;
	    int lo = from;
	    int hi = len - 1;
	    int mid = (lo + hi) / 2;
	    while (true) {
		int cmp = c.compare(arr[mid], key);
		if (cmp == 0 || cmp < 0) {
		    lo = mid + 1;
		    if (hi < lo) {
			return mid < len - 1 ? mid + 1 : -1;
		    }
		} else {
		    hi = mid - 1;
		    if (hi < lo) {
			return mid;
		    }
		}
		mid = (lo + hi) / 2;
	    }
	}
	
	public static <T> int lower_bound(T[] arr, T key, Comparator<T> c, int from, int to) {
	    int len = to;
	    if(to==-1) len = arr.length;
	    int lo = from;
	    int hi = len - 1;
	    int mid; 
	    mid = (lo + hi) / 2;
	    while (true) {
		int cmp = c.compare(arr[mid], key);
		if (cmp == 0 || cmp > 0) {
		    hi = mid - 1;
		    if (hi < lo) {
			return mid;
		    }
		} else {
		    lo = mid + 1;
		    if (hi < lo) {
			return mid < len - 1 ? mid + 1 : -1;
		    }
		}
		mid = (lo + hi) / 2;
	    }
	}
	
	public List<Triple> join(List<Tuple> input1, List<Tuple> input2) {
//		if(input1.size() < 1e2)
//		System.out.printf("Joining: \n\t%s\n\t%s\n", input1, input2);
		
		final Tuple inp1[], inp2[];
		{
		long start = System.currentTimeMillis();
		inp1 = input1.toArray(new Tuple[0]);
		inp2 = input2.toArray(new Tuple[0]);
		long end = System.currentTimeMillis();
//		System.out.printf("time conv=%d\n", end-start);
		}

		{
		long start = System.currentTimeMillis();
		java.util.Arrays.sort(inp1, cmp);
		java.util.Arrays.sort(inp2, cmp);
		long end = System.currentTimeMillis();
//		System.out.printf("time sort=%d\n", end-start);
		}

		/*
		 * From now on there is following assumption: 
		 * lists are sorted. 
		 */
		
		final int start = 0, end = inp1.length;
		
		final List<Triple> ret = Collections.synchronizedList(new LinkedList<Triple>());
		
		List<Thread> threads = Collections.synchronizedList(new LinkedList());
		int maxid = 0; 
		if(this.cores==-1) {
			maxid=input1.size() * input2.size() >= 5e5 ? Runtime.getRuntime().availableProcessors() : 0;
//			maxid = Runtime.getRuntime().availableProcessors();
		}
		for (int i = 0; i < maxid; i++) {
		    Thread thread = new Thread(new Runnable() {
			
			int start, end;
			
			public Runnable init(int id,  int maxid) {
			    start = (int)Math.floor(inp1.length*id/maxid);
			    end = (int)Math.floor(inp1.length*(id+1)/maxid);
//			    System.out.println("Thread: "+start+".."+end);
			    return this;
			}
			
			public void run() {
			    List l = handleSubset(this.start, this.end, inp1, inp2);
			    ret.addAll(l);
			}
		    }.init(i, maxid));
		    threads.add(thread);
		    thread.start();
		}
		
		for (Thread thread : threads) {
		    try {
			thread.join();
		    } catch (InterruptedException ex) {
			Logger.getLogger(SortMergeJoin.class.getName()).log(Level.SEVERE, null, ex);
		    }
		}
		return maxid>0 ? ret : handleSubset(start, end, inp1, inp2);
	}

    public List<Triple> handleSubset(int start, int end, final Tuple[] inp1, final Tuple[] inp2) {
	List<Triple> ret = new LinkedList();
	long total=0, inside=0;
	int prevId = -1;
	int idx = -1, idx2 = -1;
	for (int i = start; i < end; i++) {
		Tuple t = inp1[i];
		long s = System.currentTimeMillis();
		long s1 = System.currentTimeMillis();

		if(prevId != t.getID()) 
		{
			Tuple tu = new Tuple(t.getID(), 0);
			idx = lower_bound(inp2, tu, cmp, 0, -1);
			idx2 = upper_bound(inp2, tu, cmp, 0, -1);
		}
	    
	    long e1 = System.currentTimeMillis();
	    inside += e1-s1;
	    
	    if(idx<0) continue;
	    if(idx2<0) idx2=inp2.length;
	    
	    for (int j = idx; j < idx2; j++) {
		ret.add(new Triple(t.getID(), t.getValue(), inp2[j].getValue()));
	    }
	    
	    long e = System.currentTimeMillis();
	    total += e-s;
	}
//	System.out.printf("percentage time=%f\n", 1.0f*inside/total);
	return ret;
    }

	public static void testInterface(Join joinImpl) {
		ensureEqual(joinImpl.join(
				coerce2("a  b  c  d"), 
				coerce2("A  B  C  D")), 
				coerce3("Aa Bb Cc Dd"));
		ensureEqual(joinImpl.join( // checks whether multiple occurences are handler correctly
				coerce2("a     b  c  d"), 
				coerce2("A  A  B  C  D")), 
				coerce3("Aa Aa Bb Cc Dd"));
		ensureEqual(joinImpl.join( // is cross product working?
				coerce2("a  a  a             b        c  d"), 
				coerce2("A        A          B  B  B  C  D")), 
				coerce3("Aa Aa Aa Aa Aa Aa   Bb Bb Bb Cc Dd"));
		ensureEqual(joinImpl.join( // some more sophisticated cross product
				coerce2("a  a  a                    b  b  b           c       e e"), 
				coerce2("A        A        A        B  B              C       E")), 
				coerce3("Aa Aa Aa Aa Aa Aa Aa Aa Aa Bb Bb Bb Bb Bb Bb Cc      Ee Ee"));
		ensureEqual(joinImpl.join( // what if left null join at the end?
				coerce2("a  b"), 
				coerce2("A  B  C")), 
				coerce3("Aa Bb"));
		ensureEqual(joinImpl.join( // what if left null join at the beginning?
				coerce2("   b  c"), 
				coerce2("A  B  C")), 
				coerce3("   Bb Cc"));
		ensureEqual(joinImpl.join( // what if left null join in the middle?
				coerce2("a     c"), 
				coerce2("A  B  C")), 
				coerce3("Aa    Cc"));
		ensureEqual(joinImpl.join( // what if right null join at the end?
				coerce2("a  b  c"), 
				coerce2("A  B   ")), 
				coerce3("Aa Bb  "));
		ensureEqual(joinImpl.join( // what if right null join at the beginning?
				coerce2("a  b  c"), 
				coerce2("A  B   ")), 
				coerce3("Aa Bb  "));
		ensureEqual(joinImpl.join( // what if right null join in the middle?
				coerce2("a  b  c"), 
				coerce2("A     C")), 
				coerce3("Aa    Cc"));
	}
	
	/**
	 * Thjs functjon only verjfjes whether keys were matcher correctly 
 and number of elements jn result set js correct.
	 */
	private static boolean ensureEqual(List<Triple> joined, List<Triple> expectedResult) 
			throws RuntimeException {
		Iterator<Triple> it = expectedResult.listIterator();
		System.out.printf("Got:\n\t%s\n\tand was expecting:\n\t%s\n\n", joined, expectedResult);
		if(joined.size() != expectedResult.size()) 
			throw new RuntimeException("size is not equal "+Integer.toString(joined.size()) + " vs "+Integer.toString(expectedResult.size()));
		return true;
	}

	private static List<Tuple> coerce2(String input) {
		LinkedList<Tuple> list = new LinkedList();
		String[] split = input.split("\\W+");
		for (String chr : split) {
			if(chr.length()==0) continue;
			list.add(new Tuple(chr.toLowerCase().codePointAt(0), 0));
		}
		return list;
	}
	
	private static List<Triple> coerce3(String input) {
		LinkedList<Triple> list = new LinkedList();
		String[] split = input.split("\\W+");
		for (String chr : split) {
			if(chr.length()==0) continue;
			list.add(new Triple(chr.toLowerCase().codePointAt(0), 0, 0));
		}
		return list;
	}
}
