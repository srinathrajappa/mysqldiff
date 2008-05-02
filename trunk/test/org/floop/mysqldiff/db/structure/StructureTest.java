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

import static org.testng.AssertJUnit.assertNotNull;

import java.sql.SQLException;

import org.floop.mysqldiff.db.DatabaseConnection;
import org.testng.annotations.Test;
/**
 * Test for the structure creator
 * @author floop.pl
 */
public class StructureTest {
	@Test(groups = { "db", "db.structure" })
	public void testConnection() throws SQLException {
		DatabaseConnection connection = new DatabaseConnection("127.0.0.1;3306;root;root;mangos", "127.0.0.1;3306;root;root;mangos_udb");
		Database database1 = new Database("source", connection.getSource());
		Database database2 = new Database("destination", connection.getDestination());
		assertNotNull(database1.getTables());
		assertNotNull(database2.getTables());
		database1.assertStructureSame(database2);
		connection.cleanup();
	}
}
