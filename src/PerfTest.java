import java.util.Arrays;
import java.util.List;

/**
 * This class allows benchmarking different solutions 
 * for achieving the best performance using 
 * fluent-interface.
 * @author azochniak
 */

public class PerfTest {
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
	}
}
