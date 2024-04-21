package ICSI508_HW9;

import java.util.Set;

/**
 * An informational class which holds metadata of a table.
 * A collection of getters and setters, acting as a place to store data associated with a table.
 * @author Mike Kerr
 * @author Peter Buonaiuto
 * @author Manan Devani
 * @version 1.0
 */
public class TableMetaData {
	// Set of all column names in table
	private Set<String> attributes;
	// Set of all primary key names in table
	private Set<String> primaryKeys;
	// Set of foreign key names in table referencing another table
	private Set<String> foreignKeys;
	
	/**
	 * Constructor instantiates the values to null,
	 * until we override from DBUtils.java
	 */
	public TableMetaData() {
		this.attributes = null;
		this.primaryKeys = null;
		this.foreignKeys = null;
	}
	
	/**
	 * Takes the provided attributes and stores them in this informational class
	 * @param attributes ; Set<String>
	 */
	public void setAttributes(Set<String> attributes) {
		this.attributes = attributes;
	}
	
	/**
	 * Takes the provided PKs and stores them in this informational class
	 * @param primaryKeys ; Set<String>
	 */
	public void setPrimaryKeys(Set<String> primaryKeys) {
		this.primaryKeys = primaryKeys;
	}
	
	/**
	 * Takes the provided FKs and stores them in this informational class
	 * @param foreignKeys ; Set<String>
	 */
	public void setForeignKeys(Set<String> foreignKeys) {
		this.foreignKeys = foreignKeys;
	}
	
	/**
	 * Retrieve the attributes for the associated table.
	 * @return Set<String> attributes
	 */
	public Set<String> getAttributes(){
		return this.attributes;
	}

	/**
	 * Retrieve the PKs for the associated table.
	 * @return Set<String> primaryKeys
	 */
	public Set<String> getPrimaryKeys(){
		return this.primaryKeys;
	}

	/**
	 * Retrieve the FKs for the associated table.
	 * @return Set<String> foreignKeys
	 */
	public Set<String> getForeignKeys(){
		return this.foreignKeys;
	}
}
