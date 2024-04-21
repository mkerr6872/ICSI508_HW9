package ICSI508_HW9;

import java.util.Set;


public class TableMetaData {
	private Set<String> attributes;
	private Set<String> primaryKeys;
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
