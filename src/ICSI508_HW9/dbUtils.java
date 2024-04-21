package ICSI508_HW9;

import java.sql.*;


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
			System.out.println(this.url);
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
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	
}
