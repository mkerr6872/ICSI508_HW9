package ICSI508_HW9;

import java.sql.SQLException;

public class Demo {

	public static void main(String[] args) throws SQLException {
		
		if (args.length != 3) {
			throw new IllegalArgumentException("Please add <url>, <table1name>, <table2name> as arguments to command line");
		}
		
		else {
			String url = args[0];
			String table1 = args[1];
			String table2 = args[2];
			
			DBUtils instance = new DBUtils(url);
			instance.actualJoin(table1, table2);
			instance.estimateJoin(table1, table2);
			instance.calculateError();
			instance.display(table1, table2);
		}
	
	}
}
