package ICSI508_HW9;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;


public class dbUtils {
	String url;
	Connection conn;
	Statement st;
	
	public dbUtils(String dbURL) {
		this.url = dbURL;
		dbConnect();
	}
	
	protected void dbConnect() {
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
				System.out.println(rs.getString(1));
			}
			rs.close();
		} 
		catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	public Set<String> getAttributes(String table) {
		try {
			ResultSet rs = this.st.executeQuery("SELECT * FROM " + table);
			ResultSetMetaData rsmd = rs.getMetaData();
			int attributeCount = rsmd.getColumnCount();
			Set<String> attributeSet = new HashSet<String>();
			
			for (int i=1; i <= attributeCount; i++) {
				attributeSet.add(rsmd.getColumnName(i));
			}
			return attributeSet;
		} 
		catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
		

		
	}
	
	public void estimateJoin(String table1, String table2) {
		Set<String> attributeR = getAttributes(table1);
		Set<String> attributeS = getAttributes(table2);
		System.out.println(attributeR);
		System.out.println(attributeS);
	}
	
	
}
