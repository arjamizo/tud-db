

public class Triple {
	  private int id;
	  private int value1;
	  private int value2;

	  public Triple(int id, int value1, int value2) {
	    this.id = id;
	    this.value1 = value1;
	    this.value2 = value2;
	  }

	  public int getID() { return id; }
	  public int getValue1() { return value1; }
	  public int getValue2() { return value2; }

	@Override
	public String toString() {
		return "Triple [id=" + id + ", value1=" + value1 + ", value2=" + value2
				+ "]";
	}
	  
	  
}
