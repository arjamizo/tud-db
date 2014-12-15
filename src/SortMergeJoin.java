import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

public class SortMergeJoin implements Join{
	
	static {
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
		
		List<Triple> ret = new LinkedList();
		
		int startOfThisBin = -1, endofThisBin = -1;
		int idx = 0;
		int thisBinId = -1;
		long total=0, inside=0;
		for (Tuple t : input1) {
			idx=0;
			long s = System.currentTimeMillis();
			if(thisBinId == t.getID()) {
				long s1 = System.currentTimeMillis();
				for (int i = startOfThisBin; i < endofThisBin; i++) {
					ret.add(new Triple(t.getID(), t.getValue(), inp2[i].getValue()));
				}
				long e1 = System.currentTimeMillis();
				inside += e1-s1;
			} else {
				// Skip null joins at the beginning
				while(idx<inp2.length && inp2[idx].getID() < t.getID()) {
					idx++;
				}
				if(idx<inp2.length && inp2[idx].getID() == t.getID()) {
					thisBinId = t.getID(); 
					startOfThisBin = idx;
					while(idx<inp2.length && inp2[idx].getID() == t.getID()) {
						ret.add(new Triple(t.getID(), t.getValue(), input2.get(idx).getValue()));
						idx++;
					}
					endofThisBin = idx;
				}
			}
			long e = System.currentTimeMillis();
			total += e-s;
		}
		System.out.printf("percentage=%f", 1.0f*inside/total);
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
