import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * This class takes care about automatic testing.
 * @author azochniak
 */
public class JoinTesting {
	
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
