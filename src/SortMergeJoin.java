import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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

public class SortMergeJoin implements Join{
	
	static {
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
		
		System.out.println("<testing-framework>");
		testInterface(new SortMergeJoin());
		System.out.println("</testing-framework>");
	}

	public String getName() {
		return "Sort Merge Join";
	}
	
	public SortMergeJoin() {
	}

	public static <T> int upper_bound(T[] arr, T key, Comparator<T> c) {
	    int len = arr.length;
	    int lo = 0;
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
	
	public static <T> int lower_bound(T[] arr, T key, Comparator<T> c) {
	    int len = arr.length;
	    int lo = 0;
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
		if(input1.size() < 1e2)
		System.out.printf("Joining: \n\t%s\n\t%s\n", input1, input2);

		Comparator<Tuple> cmp = null;
		cmp = new Comparator<Tuple>() {
			public int compare(Tuple o1, Tuple o2) {
				return o1.getID()-o2.getID();
			}
		};
				
		Tuple[] inp1, inp2;
		{
		long start = System.currentTimeMillis();
		inp1 = input1.toArray(new Tuple[0]);
		inp2 = input2.toArray(new Tuple[0]);
		long end = System.currentTimeMillis();
		System.out.printf("time conv=%d\n", end-start);
		}

		{
		long start = System.currentTimeMillis();
		java.util.Arrays.sort(inp1, cmp);
		java.util.Arrays.sort(inp2, cmp);
		long end = System.currentTimeMillis();
		System.out.printf("time sort=%d\n", end-start);
		}

		/*
		 * From now on there is following assumption: 
		 * lists are sorted. 
		 */
		
		int start = 0, end = inp1.length;
		
		return handleSubset(start, end, inp1, inp2);
	}

    private List<Triple> handleSubset(int start, int end, final Tuple[] inp1, final Tuple[] inp2) {
	List<Triple> ret = new LinkedList();
	long total=0, inside=0;
	for (int i = start; i < end; i++) {
	    Tuple t = inp1[i];
	    long s = System.currentTimeMillis();
	    long s1 = System.currentTimeMillis();
	    
	    Tuple tu = new Tuple(t.getID(), 0);
	    int idx = lower_bound(inp2, tu, cmp);
	    int idx2 = upper_bound(inp2, tu, cmp);
	    
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
	System.out.printf("percentage time=%f\n", 1.0f*inside/total);
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
		for (Triple triple : joined) {
			if(!it.hasNext()) return false;
			int id = it.next().getID();
			int id2 = triple.getID();
			if(id2!=id) 
				throw new RuntimeException("ID's are not equal");
		}
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
