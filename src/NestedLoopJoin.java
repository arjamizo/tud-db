import java.util.ArrayList;
import java.util.List;

public class NestedLoopJoin implements Join{

	public String getName() {
		return "Nested Loop Join";
	}
	
	public List<Triple> join(List<Tuple> input1, List<Tuple> input2) {
		
		List<Triple> result = new ArrayList<Triple>();
				
		for (int index1 = 0; index1 < input1.size(); index1++){	
			for (int index2 = 0; index2 < input2.size(); index2++){
				
				if (input1.get(index1).getID() == input2.get(index2).getID())
					result.add(new Triple(input1.get(index1).getID(), input1.get(index1).getValue(), input2.get(index2).getValue()));
			
			}
		}
		
		return result;
	}

	

}
