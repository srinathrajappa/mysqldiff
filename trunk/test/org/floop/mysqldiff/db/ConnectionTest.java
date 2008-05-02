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

import java.sql.SQLException;

import org.testng.annotations.Test;
import static org.testng.AssertJUnit.*;
/**
 * Test for the connection holder
 * @author floop.pl
 */
public class ConnectionTest {
	@Test(groups = { "db", "db.connection" })
	public void testConnection() throws SQLException {
		DatabaseConnection connection = new DatabaseConnection("127.0.0.1;3306;root;root;mangos", "127.0.0.1;3306;root;root;mangos_udb");
		assertNotNull(connection.getSource());
		assertNotNull(connection.getDestination());
		connection.cleanup();
	}
}
