/**
 * Copyright (C) 2014
 * by 52 North Initiative for Geospatial Open Source Software GmbH
 *
 * Contact: Andreas Wytzisk
 * 52 North Initiative for Geospatial Open Source Software GmbH
 * Martin-Luther-King-Weg 24
 * 48155 Muenster, Germany
 * info@52north.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.envirocar.analyse;

import java.io.File;
import java.io.IOException;

import org.envirocar.analyse.export.csv.CSVExport;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregationAlgorithmPostgresTest {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(AggregationAlgorithmPostgresTest.class);
	
//	@Test
	public void testAlgorithm(){
		
		double maxx = 7.6339;
		double maxy = 51.96;
		double minx = 7.6224;
		double miny = 51.94799;
		
//		double maxx = 7.6539;
//		double maxy = 51.96519;
//		double minx = 7.6224;
//		double miny = 51.94799;
		        
//        AggregationAlgorithm algorithm = new AggregationAlgorithm(minx, miny, maxx, maxy, 0.00045);
		
		/*
		 * 0.00009 = 10m
		 * 0.00045 = 50m
		 * 0.00018 = 20m
		 */
        AggregationAlgorithm algorithm = new AggregationAlgorithm(0.00018);
        
		
        algorithm.runAlgorithm("53b5228ee4b01607fa566b78");
        algorithm.runAlgorithm("53b52282e4b01607fa566469");
        
		try {
			CSVExport.exportAsCSV(algorithm.getResultSet(), File.createTempFile("aggregation", ".csv").getAbsolutePath());
		} catch (IOException e) {
			LOGGER.error("Could not export resultSet as CSV:", e);
		}
		
	}
}
