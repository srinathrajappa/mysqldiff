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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
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
	private static final int	DISPLAY_EVERY				= 1000;

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
		// No primary keys? Bah... let's treat all columns as one
		if (primaryKeys.size() == 0) {
			Iterator<Column> it = columns.iterator();
			while (it.hasNext()) {
				Column c = it.next();
				primaryKeys.add(c.getName());
			}
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
		if (!this.primaryKeys.containsAll(other.primaryKeys)) {
			throw new DatabaseMismatchException("Difference in primary keys in table `" + this.name + "`");
		}
		if (!other.primaryKeys.containsAll(this.primaryKeys)) {
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

	/**
	 * Compare tables
	 * 
	 * @param other
	 *            Other database to compare to
	 * @return Updates queries
	 * @throws SQLException
	 *             In case of database problems
	 */
	public StringBuffer compare(Table other) throws SQLException {
		StringBuffer result = new StringBuffer();

		StringBuffer select = new StringBuffer();
		select.append("SELECT ");
		Iterator<Column> it = columns.iterator();
		while (it.hasNext()) {
			Column c = it.next();
			select.append("`");
			select.append(c.name);
			select.append("`");
			if (it.hasNext()) {
				select.append(", ");
			}
		}
		select.append(" FROM `");
		select.append(name);
		select.append("`");

		PreparedStatement stat = connection.prepareStatement(select.toString(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		stat.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs = stat.executeQuery(select.toString());
		ResultSetMetaData rsmd = rs.getMetaData();

		StringBuffer compareSelect = new StringBuffer();
		compareSelect.append("SELECT ");
		it = columns.iterator();
		while (it.hasNext()) {
			Column c = it.next();
			compareSelect.append("`");
			compareSelect.append(c.getName());
			compareSelect.append("`");
			if (it.hasNext()) {
				compareSelect.append(", ");
			}
		}
		compareSelect.append(" FROM `");
		compareSelect.append(name);
		compareSelect.append("` WHERE ");
		it = columns.iterator();
		int p = 1;
		while (it.hasNext()) {
			Column c = it.next();
			if (rsmd.getColumnType(p) == Types.REAL) {
				compareSelect.append("ABS(`");
				compareSelect.append(c.name);
				compareSelect.append("` - ?)<0.1");
			} else {
				compareSelect.append("`");
				compareSelect.append(c.name);
				compareSelect.append("` = ?");
			}
			if (it.hasNext()) {
				compareSelect.append(" AND ");
			}
			p++;
		}

		StringBuffer existsSelect = new StringBuffer();
		existsSelect.append("SELECT ");
		it = columns.iterator();
		while (it.hasNext()) {
			Column c = it.next();
			existsSelect.append("`");
			existsSelect.append(c.getName());
			existsSelect.append("`");
			if (it.hasNext()) {
				existsSelect.append(", ");
			}
		}		
		existsSelect.append(" FROM `");
		existsSelect.append(name);
		existsSelect.append("` WHERE ");
		Iterator<String> itK = primaryKeys.iterator();
		while (itK.hasNext()) {
			existsSelect.append("`");
			existsSelect.append(itK.next());
			existsSelect.append("` = ?");
			if (itK.hasNext()) {
				existsSelect.append(" AND ");
			}
		}
		PreparedStatement checkIfSame = other.getConnection().prepareStatement(compareSelect.toString(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		PreparedStatement checkIfExists = other.getConnection().prepareStatement(existsSelect.toString(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		int row = 0;
		StringBuffer inserts = new StringBuffer();
		inserts.append("INSERT INTO `");
		inserts.append(name);
		inserts.append("` (");
		it = columns.iterator();
		while (it.hasNext()) {
			Column c = it.next();
			inserts.append("`");
			inserts.append(c.name);
			inserts.append("`");
			if (it.hasNext()) {
				inserts.append(", ");
			}
		}
		inserts.append(") VALUES \n");

		boolean firstInsert = true;		
		
		StringBuffer updates = new StringBuffer();
		StringBuffer deletesForInserts = new StringBuffer();
		deletesForInserts.append("DELETE FROM `");
		deletesForInserts.append(name);
		deletesForInserts.append("` WHERE `");
		deletesForInserts.append(primaryKeys.iterator().next());
		deletesForInserts.append("` IN (");

		while (rs.next()) {
			row++;
			if (row % DISPLAY_EVERY == 0) {
				System.out.println("Row: " + row);
			}
			for (int f = 1; f <= columns.size(); f++) {
				checkIfSame.setString(f, rs.getString(f));
			}
			ResultSet checkResult = checkIfSame.executeQuery();
			if (!checkResult.next()) {
				// Whee, we found data that is not on the target database (or is
				// different)
				itK = primaryKeys.iterator();
				p = 1;
				while (itK.hasNext()) {
					checkIfExists.setObject(p, rs.getObject(itK.next()));
					p++;
				}
				ResultSet existsResult = checkIfExists.executeQuery();
				if (!existsResult.next()) {
					// New data
					if(!firstInsert)
					{
						inserts.append(",\n");
					}
					inserts.append("\t(");
					it = columns.iterator();
					while (it.hasNext()) {
						Column c = it.next();
						if(rs.getObject(c.getName())!=null)
						{
							inserts.append("'");
							inserts.append(quoteString(rs.getString(c.getName())));
							inserts.append("'");
						} else {
							inserts.append("NULL");
						}
						if (it.hasNext()) {
							inserts.append(", ");
						}
					}
					inserts.append(")");
					if(!firstInsert)
					{
						deletesForInserts.append(",");
					}
					deletesForInserts.append("'");
					deletesForInserts.append(rs.getString(primaryKeys.iterator().next()));
					deletesForInserts.append("'");
					firstInsert = false;
				} else {
					// Modified data
					StringBuffer update = new StringBuffer();
					update.append("UPDATE `");
					update.append(name);
					update.append("` SET ");
					boolean firstChange = true;
					it = columns.iterator();
					while (it.hasNext()) {
						Column c = it.next();
						boolean same = true;
						if(rs.getObject(c.getName())==null)
						{
							if(existsResult.getObject(c.getName())!=null)
							{
								same = false;
							}
						} else {
							same =existsResult.getString(c.getName()).equals(rs.getString(c.getName())); 
						}
						
						if(!same)
						{
							if(!firstChange)
							{
								update.append(", ");
							}
							update.append("`");
							update.append(c.getName());
							update.append("`=");
							if(rs.getObject(c.getName())!=null)
							{
								update.append("'");
								update.append(quoteString(rs.getString(c.getName())));
								update.append("'");
							} else {
								update.append("NULL");
							}
							firstChange = false;
						}
					}
					update.append(" WHERE ");
					itK = primaryKeys.iterator();
					while (itK.hasNext()) {
						String cn = itK.next();
						update.append("`");
						update.append(cn);
						update.append("` = '");
						update.append(rs.getString(cn));
						update.append("'");
						if (itK.hasNext()) {
							update.append(" AND ");
						}
					}					
					update.append(";\n");
					if(!firstChange)
					{
						System.out.println(update);
						updates.append(update);
					}
				}
				existsResult.close();
			}
		}
		checkIfSame.close();
		rs.close();
		stat.close();

		if(!firstInsert)
		{
			result.append(deletesForInserts);
			result.append(");\n");
			result.append(inserts);
			result.append(";");
		}
		result.append(updates);
		return result;
	}
	
	public String quoteString(String source)
	{
		source = source.replaceAll("\n", "\\\\n");
		source = source.replaceAll("'", "\\\\'");
		return source;
	}
}
