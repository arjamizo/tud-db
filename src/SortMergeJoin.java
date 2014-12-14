import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class SortMergeJoin implements Join{

	public String getName() {
		return "Sort Merge Join";
	}
	
	public SortMergeJoin() {
		System.out.println("<testing-framework>");
		testInterface(this);
		System.out.println("</testing-framework>");
	}
	
	public List<Triple> join(List<Tuple> input1, List<Tuple> input2) {
		System.out.printf("Joining: \n\t%s\n\t%s\n", input1, input2);
		List<Triple> ret = new LinkedList();
		for (Tuple t : input1) {
			ret.add(new Triple(0, 0, 0));
		}
		return ret;
	}
	
	public void testInterface(Join joinImpl) {
		ensureEqual(joinImpl.join(
				coerce2("a  b  c  d"), 
				coerce2("A  B  C  D")), 
				coerce3("Aa Bb Cc Dd"));
	}
	
	/**
	 * This function only verifies whether keys were matcher correctly 
	 * and number of elements in result set is correct.
	 */
	private boolean ensureEqual(List<Triple> joined, List<Triple> expectedResult) 
			throws RuntimeException {
		Iterator<Triple> it = expectedResult.listIterator();
		System.out.println(joined);
		System.out.println(expectedResult);
		if(joined.size() != expectedResult.size()) 
			throw new RuntimeException("size is not equal");
		for (Triple triple : joined) {
			if(!it.hasNext()) return false;
			int id = it.next().getID();
			int id2 = triple.getID();
			if(id2!=id) 
				throw new RuntimeException("ID's are not equal");
		}
		return true;
	}

	private List<Tuple> coerce2(String input) {
		LinkedList<Tuple> list = new LinkedList();
		String[] split = input.split("\\W+");
		for (String chr : split) {
			if(chr.length()==0) continue;
			list.add(new Tuple(chr.toLowerCase().codePointAt(0), 0));
		}
		return list;
	}
	
	private List<Triple> coerce3(String input) {
		LinkedList<Triple> list = new LinkedList();
		String[] split = input.split("\\W+");
		for (String chr : split) {
			if(chr.length()==0) continue;
			list.add(new Triple(chr.toLowerCase().codePointAt(0), 0, 0));
		}
		return list;
	}
}
