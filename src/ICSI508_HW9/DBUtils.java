package ICSI508_HW9;

import java.sql.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * This class provides utlities for interacting with a database.
 * It expects a connection url to the DB as a constructor.
 * It's primary purpose is to plug into the DB and provide insight to the
 * size of a join as well as the estimated count and the error between the two.
 * @author Mike Kerr
 * @author Peter Buonaiuto
 * @author Manan Devani
 * @version 1.0
 */
public class DBUtils {
	private String url;					// The connection uri to connect to the DB
	private Connection conn;			// The connection object, used to retrieve the Statmenet object, st, which references this connection instance.
	private Statement st;				// The Statement object executes static SQL queries with respect to the current connection
	private Integer actualJoinCount;	// True number of rows in the resulting join
	private Float estimatedJoinCount;	// Estimated number of rows from the join
	private Float error;				// The numerical error between the actual and estimated join sizes
	private String condition;			// The join condition to use specifies which estimating calculation method to use
	
	/**
	 * Constructor for the Utility class which specifies the database URL
	 * Tries to conect to the database programatically
	 * @param dbURL : connection string for db
	 */
	public DBUtils(String dbURL) {
		this.url = dbURL;
		try {
			dbConnect();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Return the true join count attribute from the class
	 * @return The true number of rows in the join
	 */
	public Integer getActualJoinCount(){
		return this.actualJoinCount;
	}

	/**
	 * Return the estimated join count attribute from the class. Based on estimation statistical calculations
	 * @return Float value of the statistical estimate
	 */
	public Float getestimatedJoinCount(){
		return this.estimatedJoinCount;
	}

	/**
	 * Getter for the class attribute that holds any error message
	 * @return String : The error message 
	 */
	public Float getError(){
		return this.error;
	}
	
	/**
	 * Attempts a connection to the database based of of the url provided through constructor.
	 * @throws SQLException : Exception thrown if the url has not been set, is invalid, or cannot connect.
	 */
	private void dbConnect() throws SQLException {
		try {
			Connection connection = DriverManager.getConnection(this.url);
			this.conn = connection;
			this.st = this.conn.createStatement();
		}
		catch(SQLException e) {
			System.err.println("Provided URL incorrect");
			throw(e);
		}
	}
	
	/**
	 * Performs a query which joins the two tables and counts the rows of the result.
	 * Stores the result in the class attribute actualJoinCount
	 * @param table1 : The first table to join
	 * @param table2 : The other table to join 
	 * @throws SQLException : Throws exception if the tables can not be found: Means the provided input tables are invalid
	 */
	public void actualJoin(String table1, String table2) throws SQLException {
		try {
			// ResultSet is an iterator that points to the first row of the results by default
			// The below aggregate function will return a single row, containing the count (num rows) of the join
			ResultSet rs = this.st.executeQuery("SELECT COUNT(*) FROM " + table1 + " NATURAL JOIN " + table2);
			while (rs.next()) {
				// getString retrieves the first column, which will contain an int value of the row count of the join
				this.actualJoinCount = Integer.parseInt(rs.getString(1));
			}
			rs.close();
		} 
		// Will throw the error if the DB could not find the tables
		catch (SQLException e) {
			System.err.println("Provided table names incorrect");
			throw(e);
		}
		
	}
	
	/**
	 * Retrieves metadata about tables and their attributes for comparison.
	 *
	 * @param table1 The name of the first table.
	 * @param table2 The name of the second table.
	 * @return TableMetaData object containing metadata information.
	 * @throws SQLException If a database access error occurs or provided table names are incorrect.
	 */
	private TableMetaData getComparisonData(String table1, String table2) throws SQLException {
		try {
			TableMetaData tableMD = new TableMetaData();	
			DatabaseMetaData databaseMD = this.conn.getMetaData();
			
			// Getting attribute names
			Set<String> attributeSet = new HashSet<String>();
			ResultSet rsa = databaseMD.getColumns(null, null, table1, null);
			while(rsa.next()) {
				attributeSet.add(rsa.getString(4));
			}
			rsa.close();
			tableMD.setAttributes(attributeSet);

			// Getting primary keys
			Set<String> primaryKeySet = new HashSet<String>();
			ResultSet rspk = databaseMD.getPrimaryKeys(null, null, table1);
			while(rspk.next()) {
				primaryKeySet.add(rspk.getString(4));
			}
			rspk.close();
			tableMD.setPrimaryKeys(primaryKeySet);
			
			// Getting foreign keys referencing table2
			Set<String> foreignKeySet = new HashSet<String>();
			ResultSet rsfk = databaseMD.getImportedKeys(null, null, table1);
			while(rsfk.next()) {
				if (rsfk.getString(3).equals(table2)) {
					foreignKeySet.add(rsfk.getString(8));
				}
			}
			rsfk.close();
			tableMD.setForeignKeys(foreignKeySet);
			
			return tableMD;
		} 
		catch (SQLException e) {
			System.err.println("Provided table names incorrect");
			throw(e);
		}
		
	}
	
	/**
	 * Retrieves the count of tuples (rows) in the specified table.
	 *
	 * @param table The name of the table.
	 * @return The count of tuples in the table.
	 * @throws SQLException If a database access error occurs or the provided table name is incorrect.
	 */
	private Integer getTupleCount(String table) throws SQLException {
		try {
			Integer tupleCount = 0;
			// Execute query to count tuples in the table
			ResultSet rs = this.st.executeQuery("SELECT COUNT(*) FROM " + table);
			while (rs.next()) {
				// Retrieve and parse count of tuples
				tupleCount = Integer.parseInt(rs.getString(1));
			}
			rs.close(); // Close the result set
			return tupleCount; // Return the count of tuples
		} catch (SQLException e) {
			System.err.println("Provided table name incorrect"); // Print error message
			throw(e); // Rethrow SQLException
		}	
	}

	
	/**
	 * Retrieves the count of distinct tuples (rows) in the specified table based on the provided attributes.
	 *
	 * @param table The name of the table.
	 * @param attributes A set containing the names of attributes based on which distinct tuples are counted.
	 * @return The count of distinct tuples in the table based on the specified attributes.
	 * @throws SQLException If a database access error occurs or the provided table name is incorrect.
	 */
	private Integer getDistinctTupleCountByAttribute(String table, Set<String> attributes) throws SQLException {
		try {
			Integer attributeCount = attributes.size(); // Get the number of attributes
			Iterator<String> attributeIterator = attributes.iterator(); // Iterator for attributes
			Integer count = 0; // Counter for attributes processed
			StringBuilder attributeString = new StringBuilder(""); // StringBuilder to build attribute string

			// Construct comma-separated attribute string
			while(attributeIterator.hasNext()) {
				attributeString.append(attributeIterator.next()); // Append attribute
				count++;
				if (count < attributeCount) {
					attributeString.append(","); // Add comma if not the last attribute
				}
			}

			String queryAttribute = attributeString.toString(); // Convert StringBuilder to String

			Integer tupleCount = 0; // Initialize tuple count
			// Execute query to count distinct tuples based on attributes
			ResultSet rs = this.st.executeQuery("SELECT COUNT(*) FROM (SELECT " + queryAttribute + " FROM " + table + " GROUP BY " + queryAttribute + ")");
			while (rs.next()) {
				tupleCount = Integer.parseInt(rs.getString(1)); // Retrieve and parse count of distinct tuples
			}
			rs.close(); // Close the result set
			return tupleCount; // Return the count of distinct tuples
		} catch (SQLException e) {
			System.err.println("Provided table name incorrect"); // Print error message
			throw(e); // Rethrow SQLException
		}	
	}

	
	/**
	 * Estimates the join count between two tables and determines the join condition based on their metadata.
	 *
	 * @param table1 The name of the first table.
	 * @param table2 The name of the second table.
	 * @throws SQLException If a database access error occurs or the provided table names are incorrect.
	 */
	public void estimateJoin(String table1, String table2) throws SQLException {
		TableMetaData table1MD = getComparisonData(table1, table2); // Metadata for table 1
		TableMetaData table2MD = getComparisonData(table2, table1); // Metadata for table 2
		
		// Find intersection of attributes between table 1 and table 2
		Set<String> attributeIntersection = new HashSet<String>(table1MD.getAttributes());
		attributeIntersection.retainAll(table2MD.getAttributes());
		
		// No intersection of attributes
		if (attributeIntersection.isEmpty()) {
			System.out.println("Empty set, n1 * n2");
			Integer n1 = getTupleCount(table1); // Count of tuples in table 1
			Integer n2 = getTupleCount(table2); // Count of tuples in table 2
			this.estimatedJoinCount = (float) (n1 * n2); // Estimate join count
			this.condition = "NullSet"; // Set join condition
		}
		
		// Intersection of attributes is a foreign key of table 1 referencing table 2
		else if (attributeIntersection.equals(table1MD.getForeignKeys())) {
			this.estimatedJoinCount = (float) getTupleCount(table1); // Estimate join count using table 1
			this.condition = "FK"; // Set join condition
		}
		
		// Intersection of attributes is a foreign key of table 2 referencing table 1
		else if (attributeIntersection.equals(table2MD.getForeignKeys())) {
			this.estimatedJoinCount = (float) getTupleCount(table2); // Estimate join count using table 2
			this.condition = "FK"; // Set join condition
		}
		
		// Intersection of attributes is a key of table 1
		else if (attributeIntersection.equals(table1MD.getPrimaryKeys())) {
			this.estimatedJoinCount = (float) getTupleCount(table2); // Estimate join count using table 2
			this.condition = "PK"; // Set join condition
		}
		
		// Intersection of attributes is a key of table 2
		else if (attributeIntersection.equals(table2MD.getPrimaryKeys())) {
			this.estimatedJoinCount = (float) getTupleCount(table1); // Estimate join count using table 1
			this.condition = "PK"; // Set join condition
		}
		
		// Intersection of attributes is not a key for table 1 or table 2
		else {
			Integer n1 = getTupleCount(table1); // Count of tuples in table 1
			Integer n2 = getTupleCount(table2); // Count of tuples in table 2
			Integer VAtable1 = getDistinctTupleCountByAttribute(table1, attributeIntersection); // Count of distinct tuples in table 1
			Integer VAtable2 = getDistinctTupleCountByAttribute(table2, attributeIntersection); // Count of distinct tuples in table 2
			// Estimate join count based on minimum distinct tuple count
			this.estimatedJoinCount = (Math.min((float) n1 * n2 / VAtable1, (float) n1 * n2 / VAtable2));
			this.condition = "NA"; // Set join condition
		}
	}

	/**
	 * Calculate the error between the estimated and actual join count. 
	 * Sets the result into class level field "error"
	 */
	public void calculateError() {
		this.error = this.estimatedJoinCount - this.actualJoinCount;
	}
	
	/**
	 * Display the results of joining the two tables together
	 * Will provide the true count, estimation, and error.
	 * @param table1 : The first table name (used only to display as a string)
	 * @param table2 : The other table name (used only to display as a string)
	 */
	public void display(String table1, String table2) {
		System.out.println("For the Natural Joining of " + table1 + " and " + table2);
		switch (this.condition) {
			case "PK":
				System.out.println("Estimated Join Size <= " + this.estimatedJoinCount);
				break;
			default:  
				System.out.println("Estimated Join Size = " + this.estimatedJoinCount);
		}
		System.out.println("Actual Join Size = " + this.actualJoinCount);
		System.out.println("Estimation Error = " + this.error);
	}
	
}
