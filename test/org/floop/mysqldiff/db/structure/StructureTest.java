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

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;

import org.floop.mysqldiff.db.DatabaseConnection;
import org.testng.annotations.Test;
/**
 * Test for the structure creator
 * @author floop.pl
 */
public class StructureTest {
	@Test(groups = { "db", "db.structure" })
	public void testConnection() throws SQLException {
		DatabaseConnection connection = new DatabaseConnection("127.0.0.1;3306;root;root;mangos_udb", "127.0.0.1;3306;root;root;mangos_off");
		Database database1 = new Database("source", connection.getSource());
		Database database2 = new Database("destination", connection.getDestination());
		ArrayList<String>names = new ArrayList<String>();
		/*
		names.add("creature");
		names.add("creature_template");
		names.add("creature_movement");
		names.add("command");
		*/
		names.add("creature");
		names.add("creature_movement");		
		StringBuffer result = database1.compare(database2, names);
		FileOutputStream out; // declare a file output object
		PrintStream p; // declare a print stream object
		try {
			out = new FileOutputStream("result.sql");
			// Connect print stream to the output stream
			p = new PrintStream(out);
			p.println(result.toString());
			p.close();
		} catch (Exception e) {
			System.err.println("Error writing to file ");
		}		
		connection.cleanup();
	}
}
