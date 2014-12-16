

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Starter {
	
	public static List<Tuple> cloneList(List<Tuple> list) {
	    List<Tuple> clone = new ArrayList<Tuple>(list.size());
	    for(Tuple item: list) clone.add(item.clone());
	    return clone;
	}
	
	public static void main(String[] args) {
	    
		//this could change for the final test
		
		int size = 1000;
		int distinctValues = 10000;
		
		//no changes from here on
		if (args.length >= 2) {
			size = Integer.parseInt(args[0]);
			distinctValues = Integer.parseInt(args[1]);
		}
		
		List<Tuple> input1 = new ArrayList<Tuple>();
		List<Tuple> input2 = new ArrayList<Tuple>();
		
		Random rand = new Random();
		
		for (int i = 0; i < size; i++){
			input1.add(new Tuple(rand.nextInt(distinctValues), i));
			input2.add(new Tuple(rand.nextInt(distinctValues), i));
		}
		
		long start, stop;
		
		System.out.println("Could use " + Runtime.getRuntime().availableProcessors() + " cores.");
		
		List<Join> joins = new ArrayList<Join>();
		joins.add(new NestedLoopJoin());
		joins.add(new SortMergeJoin());
		joins.add(new HashJoin());
		
		long[] times = new long[joins.size()];
		
		System.out.println("Total Tuples   : " + size);
		System.out.println("Distinct Values: " + distinctValues);
		System.out.println();
		
		for (int i = 0; i < joins.size(); i++){
		
			List<Tuple> tempInput1 = cloneList(input1);
			List<Tuple> tempInput2 = cloneList(input2);
			
			
			System.out.println("Doing "+joins.get(i).getName() + ", please wait ...");
			start = System.currentTimeMillis();
			List<Triple> result = joins.get(i).join( tempInput1, tempInput2);
			stop = System.currentTimeMillis();
			
			times[i] = (stop-start);
			if (result == null) System.out.println("Result is NULL!");
			else {
			System.out.println("Size:     " + result.size());
			System.out.println("Time(ms): " + times[i]);
			if(times[i]!=0)
			System.out.println("Speedup : " + (times[0]/times[i]) + " x");
			}
			
			System.out.println("");
			
		}
	
		
	}
	
	
	
}