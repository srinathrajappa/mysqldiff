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
package org.floop.mysqldiff.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * DatabaseConnection holder class
 * 
 * @author floop.pl
 */
public class DatabaseConnection {
	private int			SERVER					= 0;
	private int			PORT					= 1;
	private int			LOGIN					= 2;
	private int			PASSWORD				= 3;
	private int			DATABASE				= 4;

	private Connection	sourceConnection		= null;
	private Connection	destinationConnection	= null;

	/**
	 * Constructor for database connections. Database connection is provided in
	 * format: server;port;login;password;database
	 * 
	 * @param source
	 *            DatabaseConnection to source database
	 * @param destination
	 *            DatabaseConnection to destination database
	 * @throws SQLException
	 *             If you can't connect to database
	 */
	public DatabaseConnection(String source, String destination) throws SQLException {
		String sourceSplitted[] = source.split(";");
		if (sourceSplitted.length != 5) {
			throw new RuntimeException("Wrong format of source connection string (" + source + "), expected of server;port;login;password;database");
		}
		String destinationSplitted[] = destination.split(";");
		if (destinationSplitted.length != 5) {
			throw new RuntimeException("Wrong format of destination connection string (" + destination + "), expected of server;port;login;password;database");
		}
		try {
			Class.forName("org.gjt.mm.mysql.Driver").newInstance();
		} catch (Exception E) {
			throw new RuntimeException("Cannot load org.gjt.mm.mysql.Driver");
		}
		sourceConnection = DriverManager.getConnection(connectionBuild(sourceSplitted));
		destinationConnection = DriverManager.getConnection(connectionBuild(destinationSplitted));
	}
	
	public void cleanup()
	{
		if(sourceConnection!=null)
		{
			try {
				sourceConnection.close();				
			} catch (SQLException e)
			{
				// Gobble this exception on purpose
			}
			sourceConnection = null;
		}
		if(destinationConnection!=null)
		{
			try {
				destinationConnection.close();				
			} catch (SQLException e)
			{
				// Gobble this exception on purpose
			}
			destinationConnection = null;
		}
	}

	/**
	 * Build the connection in format jdbc:mysql://[host][:port]/[database]
	 */
	private String connectionBuild(String[] splittedConnection) {
		StringBuffer connectionString = new StringBuffer();
		connectionString.append("jdbc:mysql://");
		connectionString.append(splittedConnection[SERVER]);
		connectionString.append(":");
		connectionString.append(splittedConnection[PORT]);
		connectionString.append("/");
		connectionString.append(splittedConnection[DATABASE]);
		connectionString.append("?user=");
		connectionString.append(splittedConnection[LOGIN]);
		connectionString.append("&password=");
		connectionString.append(splittedConnection[PASSWORD]);		
		return connectionString.toString();
	}

	public Connection getSource() {
		return sourceConnection;
	}

	public Connection getDestination() {
		return destinationConnection;
	}
}
