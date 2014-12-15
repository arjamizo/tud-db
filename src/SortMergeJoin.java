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

		ListIterator<Tuple> it = input2.listIterator();
		Tuple ti = it.next();
		
		List<Triple> ret = new LinkedList();
		
		for (Tuple t : input1) {
			boolean was = false;
			while (ti.getID() == t.getID()) {
				ret.add(new Triple(t.getID(), t.getValue(), ti.getValue()));
				was = true;
				if(!it.hasNext()) break;
				ti=it.next();
			};
			if(was) {
				do {
					ti = it.previous();
				} while (ti.getID() == t.getID() && it.hasPrevious());
				ti=it.next();
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
