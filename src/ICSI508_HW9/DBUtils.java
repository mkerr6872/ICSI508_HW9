package ICSI508_HW9;

import java.sql.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


public class DBUtils {
	private String url;
	private Connection conn;
	private Statement st;
	private Integer actualJoinCount;
	private Float estimatedJoinCount;
	private Float error;
	private String condition;
	
	// Constructor
	public DBUtils(String dbURL) {
		this.url = dbURL;
		try {
			dbConnect();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public Integer getActualJoinCount(){
		return this.actualJoinCount;
	}

	public Float getestimatedJoinCount(){
		return this.estimatedJoinCount;
	}
	public Float getError(){
		return this.error;
	}
	
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
	
	public void actualJoin(String table1, String table2) throws SQLException {
		try {
			ResultSet rs = this.st.executeQuery("SELECT COUNT(*) FROM " + table1 + " NATURAL JOIN " + table2);
			while (rs.next()) {
				this.actualJoinCount = Integer.parseInt(rs.getString(1));
			}
			rs.close();
		} 
		catch (SQLException e) {
			System.err.println("Provided table names incorrect");
			throw(e);
		}
		
	}
	
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
	
	private Integer getTupleCount(String table) throws SQLException {
		try {
			Integer tupleCount = 0;
			ResultSet rs = this.st.executeQuery("SELECT COUNT(*) FROM " + table);
			while (rs.next()) {
				tupleCount = Integer.parseInt(rs.getString(1));
			}
			rs.close();
			return tupleCount;
		} 
		catch (SQLException e) {
			System.err.println("Provided table names incorrect");
			throw(e);
		}	
	}
	
	private Integer getDistinctTupleCountByAttribute(String table, Set<String> attributes) throws SQLException {
		try {
			Integer attributeCount = attributes.size();
			Iterator<String> attributeIterator = attributes.iterator(); 
			Integer count = 0;
			StringBuilder attributeString = new StringBuilder("");
			while(attributeIterator.hasNext()) {
				attributeString.append(attributeIterator.next());
				count++;
				if (count < attributeCount) {
					attributeString.append(",");
				}
			}
			
			String queryAttribute = attributeString.toString();
			
			Integer tupleCount = 0;
			ResultSet rs = this.st.executeQuery("SELECT COUNT(*) FROM (SELECT " + queryAttribute + " FROM " + table + " GROUP BY " + queryAttribute + ")");
			while (rs.next()) {
				tupleCount = Integer.parseInt(rs.getString(1));
			}
			rs.close();
			return tupleCount;
		} 
		catch (SQLException e) {
			System.err.println("Provided table names incorrect");
			throw(e);
		}	
	}
	
	
	public void estimateJoin(String table1, String table2) throws SQLException {
		TableMetaData table1MD = getComparisonData(table1, table2);
		TableMetaData table2MD = getComparisonData(table2, table1);
		
		// Find intersection of attributes
		Set<String> attributeIntersection = new HashSet<String>(table1MD.getAttributes());
		attributeIntersection.retainAll(table2MD.getAttributes());
		
		// No intersection of attributes
		if (attributeIntersection.isEmpty()) {
			System.out.println("Empty set, n1 * n2");
			Integer n1 = getTupleCount(table1);
			Integer n2 = getTupleCount(table2);
			this.estimatedJoinCount = (float) (n1*n2);
			this.condition = "NullSet";
		}
		
		// Intersection of attributes is a foreign key of table 1 referencing table 2
		else if (attributeIntersection.equals(table1MD.getForeignKeys())) {
			this.estimatedJoinCount = (float) getTupleCount(table1);
			this.condition = "FK";
		}
		
		// Intersection of attributes is a foreign key of table 2 referencing table 1
		else if (attributeIntersection.equals(table2MD.getForeignKeys())) {
			this.estimatedJoinCount = (float) getTupleCount(table2);
			this.condition = "FK";
		}
		
		// Intersection of attributes is a key of table 1
		else if (attributeIntersection.equals(table1MD.getPrimaryKeys())) {
			this.estimatedJoinCount = (float) getTupleCount(table2);
			this.condition = "PK";
		}
		
		// Intersection of attributes is a key of table 2
		else if (attributeIntersection.equals(table2MD.getPrimaryKeys())) {
			this.estimatedJoinCount = (float) getTupleCount(table1);
			this.condition = "PK";
		}
		
		// Intersection of attributes is not a key for table 1 or table 2
		else {
			Integer n1 = getTupleCount(table1);
			Integer n2 = getTupleCount(table2);
			Integer VAtable1 = getDistinctTupleCountByAttribute(table1, attributeIntersection);
			Integer VAtable2 = getDistinctTupleCountByAttribute(table2, attributeIntersection);
			this.estimatedJoinCount = (Math.min((float)n1*n2/VAtable1 , (float)n1*n2/VAtable2));
			this.condition = "NA";
		}

	}
	
	public void calculateError() {
		this.error = this.estimatedJoinCount - this.actualJoinCount;
	}
	
	
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
