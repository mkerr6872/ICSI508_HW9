package ICSI508_HW9;

import java.util.Set;


public class TableMetaData {
	// Set of all column names in table
	private Set<String> attributes;
	// Set of all primary key names in table
	private Set<String> primaryKeys;
	// Set of foreign key names in table referencing another table
	private Set<String> foreignKeys;
	
	public TableMetaData() {
		this.attributes = null;
		this.primaryKeys = null;
		this.foreignKeys = null;
	}
	
	public void setAttributes(Set<String> attributes) {
		this.attributes = attributes;
	}
	
	public void setPrimaryKeys(Set<String> primaryKeys) {
		this.primaryKeys = primaryKeys;
	}
	
	public void setForeignKeys(Set<String> foreignKeys) {
		this.foreignKeys = foreignKeys;
	}
	
	public Set<String> getAttributes(){
		return this.attributes;
	}

	public Set<String> getPrimaryKeys(){
		return this.primaryKeys;
	}
	public Set<String> getForeignKeys(){
		return this.foreignKeys;
	}
}
