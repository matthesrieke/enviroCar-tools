/**
 * Copyright (C) 2013
 * by 52 North Initiative for Geospatial Open Source Software GmbH
 *
 * Contact: Andreas Wytzisk
 * 52 North Initiative for Geospatial Open Source Software GmbH
 * Martin-Luther-King-Weg 24
 * 48155 Muenster, Germany
 * info@52north.org
 *
 * This program is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public License version 2 as published by the
 * Free Software Foundation.
 *
 * This program is distributed WITHOUT ANY WARRANTY; even without the implied
 * WARRANTY OF MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program (see gnu-gpl v2.txt). If not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA or
 * visit the Free Software Foundation web page, http://www.fsf.org.
 */
package org.envirocar.analyse;

import java.io.File;
import java.io.IOException;

import org.envirocar.analyse.export.csv.CSVExport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregationAlgorithmPostgresTest {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(AggregationAlgorithmPostgresTest.class);
	
	public static void main(String[] args) throws IOException{
		
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
