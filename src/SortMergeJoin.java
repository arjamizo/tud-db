import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

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
	
	public List<Triple> join(List<Tuple> input1, List<Tuple> input2) {
		if(input1.size() < 1e2)
		System.out.printf("Joining: \n\t%s\n\t%s\n", input1, input2);

		Comparator<Tuple> cmp = null;
		cmp = new Comparator<Tuple>() {
			public int compare(Tuple o1, Tuple o2) {
				return o1.getID()-o2.getID();
			}
		};
		
		java.util.Collections.sort(input1, cmp);
		java.util.Collections.sort(input2, cmp);

		/*
		 * From now on there is following assumption: 
		 * lists are sorted. 
		 */
		
		List<Triple> ret = new LinkedList();
		
		for (Tuple t : input1) {
			int idx = -1;
			for (Tuple t2 : input2) {
				if (cmp.compare(t, t2)==0) {
					idx = input2.indexOf(t2);
					break;
				}
			}
			if(idx!=-1) {
				while(input2.get(idx).getID() == t.getID()) {
					ret.add(new Triple(t.getID(), t.getValue(), input2.get(idx).getValue()));
					if(idx+1 >= input2.size()) break;
					idx++;
				}
			}
		}
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
	 * This function only verifies whether keys were matcher correctly 
	 * and number of elements in result set is correct.
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
