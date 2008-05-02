/**
 * Copyright 2008 floop.pl
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *  
 * http://www.apache.org/licenses/LICENSE-2.0 
 *  
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 */
package org.floop.mysqldiff.db.structure;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

import org.floop.mysqldiff.db.exceptions.DatabaseMismatchException;

/**
 * Object containing the database
 * 
 * @author floop.pl
 */
public class Database extends DatabaseConnectionAndName {
	private static final int	METADATA_RESULT_TABLE_NAME	= 3;
	private ArrayList<Table>	tables						= new ArrayList<Table>();

	/**
	 * Constructor for Database object
	 * 
	 * @param connection
	 * @throws SQLException
	 *             In case of database problems
	 */
	public Database(String name, Connection connection) throws SQLException {
		super(name, connection);
		DatabaseMetaData md = connection.getMetaData();
		ResultSet rs = md.getTables(null, null, "%", null);
		while (rs.next()) {
			tables.add(new Table(rs.getString(METADATA_RESULT_TABLE_NAME), connection));
		}		
	}

	public ArrayList<Table> getTables() {
		return tables;
	}
	
	/**
	 * This function checks the structure of database and asserts it is same like the other database provided
	 * @param other Other database to compare to
	 */
	public void assertStructureSame(Database other)
	{
		if(this.tables.size()!=other.tables.size())
		{
			throw new DatabaseMismatchException("Different number of tables: " + this.tables.size() + "<>" + other.tables.size());
		}
		Iterator<Table> itThis  = tables.iterator();
		Iterator<Table> itOther = other.tables.iterator();
		while(itThis.hasNext())
		{
			Table thisTable = itThis.next();
			Table otherTable = itOther.next();
			if(!thisTable.name.equals(otherTable.name))
			{
				throw new DatabaseMismatchException("Different table names: " + thisTable.name + "<>" + otherTable.name);
			}
			thisTable.assertStructureSame(otherTable);
		}
	}
}
