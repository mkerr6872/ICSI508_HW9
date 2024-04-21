package ICSI508_HW9;

public class Demo {

	public static void main(String[] args) {
		
		if (args.length != 3) {
			System.out.println("Please add the url, table1, and table2 as command line arguments");
		}
		
		else {
			String url = args[0];
			String table1 = args[1];
			String table2 = args[2];
			
			DBUtils instance = new DBUtils(url);
			instance.actualJoin(table1, table2);
			instance.estimateJoin(table1, table2);
		}
	
	}
}
