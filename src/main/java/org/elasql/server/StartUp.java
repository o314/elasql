/*******************************************************************************
 * Copyright 2016 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.elasql.server;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.procedure.DdSampleSpFactory;

public class StartUp {
	private static Logger logger = Logger.getLogger(StartUp.class.getName());

	public static void main(String args[]) throws Exception {
		if (logger.isLoggable(Level.INFO))
			logger.info("initing...");
		
		// For initializing VanillaDb
		boolean isSeq = false;
		if (args.length > 2) {
			int num = Integer.parseInt(args[2]);
			if (num == 1)
				isSeq = true; 
		}
		
		// configure and initialize the database
		Elasql.init(args[0], Integer.parseInt(args[1]), isSeq, new DdSampleSpFactory());

		if (logger.isLoggable(Level.INFO))
			logger.info("dd database server ready");
	}
}
