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
 * Object containing the table
 * 
 * @author floop.pl
 */
public class Table extends DatabaseConnectionAndName {
	private static final int	METADATA_RESULT_COLUMN_NAME	= 4;
	private ArrayList<Column>	columns						= new ArrayList<Column>();
	private ArrayList<String>	primaryKeys					= new ArrayList<String>();

	public Table(String name, Connection connection) throws SQLException {
		super(name, connection);
		DatabaseMetaData md = connection.getMetaData();
		ResultSet rs = md.getColumns(null, null, name, "%");
		while (rs.next()) {
			columns.add(new Column(rs.getString(METADATA_RESULT_COLUMN_NAME), connection));
		}
		rs = md.getPrimaryKeys(null, null, name);
		while (rs.next()) {
			primaryKeys.add(rs.getString(METADATA_RESULT_COLUMN_NAME));
		}
	}

	public ArrayList<String> getPrimaryKeys() {
		return primaryKeys;
	}

	public ArrayList<Column> getColumns() {
		return columns;
	}

	public void assertStructureSame(Table other) {
		if (this.columns.size() != other.columns.size()) {
			throw new DatabaseMismatchException("Different number of columns in table `" + this.name + "`: " + this.columns.size() + "<>" + other.columns.size());
		}
		if(!this.primaryKeys.containsAll(other.primaryKeys))
		{
			throw new DatabaseMismatchException("Difference in primary keys in table `" + this.name + "`");
		}
		if(!other.primaryKeys.containsAll(this.primaryKeys))
		{
			throw new DatabaseMismatchException("Difference in primary keys in table `" + this.name + "`");
		}
		Iterator<Column> itThis = columns.iterator();
		Iterator<Column> itOther = other.columns.iterator();
		while (itThis.hasNext()) {
			Column thisColumn = itThis.next();
			Column otherColumn = itOther.next();
			if (!thisColumn.name.equals(otherColumn.name)) {
				throw new DatabaseMismatchException("Different columns in table `" + this.name + "`: " + thisColumn.name + "<>" + otherColumn.name);
			}
		}

	}
}
