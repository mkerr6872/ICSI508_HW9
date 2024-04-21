package ICSI508_HW9;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;


public class DBUtils {
	private String url;
	private Connection conn;
	private Statement st;
	private Integer actualJoinCount;
	private Integer estimatedJointCount;
	
	public DBUtils(String dbURL) {
		this.url = dbURL;
		dbConnect();
	}
	
	private void dbConnect() {
		try {
			this.conn = DriverManager.getConnection(this.url);
			this.st = this.conn.createStatement();
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void actualJoin(String table1, String table2) {
		try {
			ResultSet rs = this.st.executeQuery("SELECT COUNT(*) FROM " + table1 + " NATURAL JOIN " + table2);
			while (rs.next()) {
				this.actualJoinCount = Integer.parseInt(rs.getString(1));
				System.out.println("Actual join count: " + this.actualJoinCount);
			}
			rs.close();
		} 
		catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	private TableMetaData getComparisonData(String table1, String table2) {
		try {
			TableMetaData tableMD = new TableMetaData();	
			ResultSet rs = this.st.executeQuery("SELECT * FROM " + table1);
			ResultSetMetaData rsmd = rs.getMetaData();
			
			// Getting attribute names
			Set<String> attributeSet = new HashSet<String>();
			int attributeCount = rsmd.getColumnCount();;
			for (int i=1; i <= attributeCount; i++) {
				attributeSet.add(rsmd.getColumnName(i));
			}
			tableMD.setAttributes(attributeSet);

			// Getting primary keys
			Set<String> primaryKeySet = new HashSet<String>();
			DatabaseMetaData databaseMD = this.conn.getMetaData();
			ResultSet rspk = databaseMD.getPrimaryKeys(null, null, table1);
			while(rspk.next()) {
				primaryKeySet.add(rspk.getString(4));
			}
			tableMD.setPrimaryKeys(primaryKeySet);
			
			// Getting foreign keys referencing table2
			Set<String> foreignKeySet = new HashSet<String>();
			ResultSet rsfk = databaseMD.getImportedKeys(null, null, table1);
			while(rsfk.next()) {
				if (rsfk.getString(3).equals(table2)) {
					foreignKeySet.add(rsfk.getString(8));
				}
			}
			tableMD.setForeignKeys(foreignKeySet);
			
			
			return tableMD;
		} 
		catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
	public void estimateJoin(String table1, String table2) {
		TableMetaData table1MD = getComparisonData(table1, table2);
		TableMetaData table2MD = getComparisonData(table2, table1);
		
		
		//System.out.println(table1MD.getAttributes());
		//System.out.println(table1MD.getPrimaryKeys());
		//System.out.println(table1MD.getForeignKeys());

	}
	
	
}
