package org.envirocar.analyse.export.csv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.envirocar.analyse.entities.Point;
import org.envirocar.analyse.properties.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVExport {

	private static final Logger LOGGER = LoggerFactory.getLogger(CSVExport.class);
	
	public static void exportAsCSV(List<Point> resultSet, String fileName) {
		
		LOGGER.debug("CSV export file name: " + fileName);
		
		File csvFile = new File(fileName);
		
		BufferedWriter bufferedWriter = null;
		
		try {
			bufferedWriter = new BufferedWriter(new FileWriter(csvFile));
			
			bufferedWriter.write(createCSVHeader());
			
			for (Point point : resultSet) {
				bufferedWriter.write(createCSVLine(point));
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				bufferedWriter.close();
			} catch (IOException e) {}
		}
	}

	private static String createCSVHeader() {
		
		String result = "ID;lon;lat;NumberOfPointsUsed;NumberOfTracksUsed";
		
		Iterator<String> propertyNameIterator = Properties.getPropertiesOfInterestDatatypeMapping().keySet().iterator();
				
		while (propertyNameIterator.hasNext()) {
			String propertyName = (String) propertyNameIterator.next();
			
			result = result.concat(propertyName);
			
			if(propertyNameIterator.hasNext()){
				result = result.concat(";");
			}			
			
		}
		
		result = result.concat("\n");
		
		return result;
	}

	private static String createCSVLine(Point point){
		
		String result = point.getID() + ";" + point.getX() + ";" + point.getY() + ";" + point.getNumberOfPointsUsedForAggregation()+ ";" + point.getNumberOfTracksUsedForAggregation() + ";";
		
		Iterator<String> propertyNameIterator = Properties.getPropertiesOfInterestDatatypeMapping().keySet().iterator();
		
		Map<String, Object> propertyMap = point.getPropertyMap();
		
		while (propertyNameIterator.hasNext()) {
			String propertyName = (String) propertyNameIterator.next();
			
			result = result.concat(String.valueOf(propertyMap.get(propertyName)));
			
			if(propertyNameIterator.hasNext()){
				result = result.concat(";");
			}			
			
		}
		
		result = result.concat("\n");
		
		return result;		
	}
	
}
