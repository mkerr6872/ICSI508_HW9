package ICSI508_HW9;

public class demo {

	public static void main(String[] args) {
		
		if (args.length != 3) {
			System.out.println("Please add the url, table1, and table2 as command line arguments");
		}
		
		else {
			dbUtils instance = new dbUtils(args[0]);
			instance.actualJoin(args[1], args[2]);
		}
	
	}
}
