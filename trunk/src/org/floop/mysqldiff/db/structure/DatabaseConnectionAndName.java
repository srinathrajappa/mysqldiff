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

/**
 * Generic object to hold name and database connection.
 * 
 * @author floop.pl
 */
public class DatabaseConnectionAndName {
	protected String		name;
	protected Connection	connection;
	
	public DatabaseConnectionAndName(String name, Connection connection) {
		super();
		this.name = name;
		this.connection = connection;
	}
	
	public String getName() {
		return name;
	}
	public Connection getConnection() {
		return connection;
	}
}
