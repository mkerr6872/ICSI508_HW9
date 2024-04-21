# ICSI508_HW9

This project implements functionality to estimate the join size between two tables in a database using statistical calculations. It includes a utility class `DBUtils` to perform the join estimation based on metadata about the tables and their attributes.

## Authors
- Mike
- Peter Buonaiuto
- Manan Devani

## Usage

### Running the Demo

To run the demo, execute the `Demo` class with the required arguments:
```
java ICSI508_HW9.Demo <db_url> <table1_name> <table2_name>
```
Replace `<db_url>`, `<table1_name>`, and `<table2_name>` with the appropriate values.

### Arguments
- `<db_url>`: The URL of the database.
- `<table1_name>`: The name of the first table to join.
- `<table2_name>`: The name of the second table to join.

### Example

java ICSI508_HW9.Demo jdbc:postgresql://localhost:5432/mydatabase table1 table2

## Requirements
- Java 8 or higher
- PostgreSQL JDBC driver

## License
This project is licensed under the MIT License. 

## GitHub Repository
Find the source code and updates on GitHub: [https://github.com/mkerr6872/ICSI508_HW9](https://github.com/mkerr6872/ICSI508_HW9).
