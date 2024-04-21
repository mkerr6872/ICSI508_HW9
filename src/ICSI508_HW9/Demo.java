package ICSI508_HW9;

import java.sql.SQLException;

/**
 * Our driver class which demonstrates the functionality of the DBUtils.java class.
 * It requires the database url and two tables to perform the query on, and throws error otherwise.
 * The overall result will be terminal logs from DBUtils providing the sizes of the join operation
 * @author Mike Kerr
 * @author Peter Buonaiuto
 * @author Manan Devani
 * @version 1.0
 */
public class Demo {
	/**
	 * Main method from the terminal: Takes arguments, validates them and runs DBUtils functions 
	 * to provide the comparsion between true and estimated join sizes of the two tables from the given DB
	 * @param args : <db url>, <table1name>, <table2name> : The database and it's two tables to join.
	 * @throws SQLException : Propogates any SQL errors from DBUtils up to our main Driver (Demo)
	 */
	public static void main(String[] args) throws SQLException {
		
		// Make sure the user provided all necessary arguments. Otherwise, we can't run the program
		if (args.length != 3) {
			throw new IllegalArgumentException("Please add <url>, <table1name>, <table2name> as arguments to command line");
		}
		
		// Arguments are good, parse them and pass to a new instance of DBUtils.java
		else {
			String url = args[0];
			String table1 = args[1];
			String table2 = args[2];
			
			// instance object holds our functionality for this program.
			DBUtils instance = new DBUtils(url);

			// Instantiates the field of the actual join count in DBUtils
			instance.actualJoin(table1, table2); 

			// Instantiates the field of the estimated join count in DBUtils
			instance.estimateJoin(table1, table2);

			// Instantiates the field of the error in estimated join count in DBUtils
			instance.calculateError();

			// Displays the aforementioned attributes from DBUtils.java, providing the statistics.
			instance.display(table1, table2);
		}
	
	}
}
