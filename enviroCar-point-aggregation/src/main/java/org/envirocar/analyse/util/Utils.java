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
package org.envirocar.analyse.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.envirocar.analyse.properties.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vividsolutions.jts.geom.GeometryFactory;

public class Utils {
    
	private static final Logger LOGGER = LoggerFactory
			.getLogger(Utils.class);
	
	public static GeometryFactory geometryFactory = new GeometryFactory();
	
    public static double[] convertWKTPointToXY(String wktPointAsString){
    	
    	double[] result = new double[2];
    	
    	wktPointAsString = wktPointAsString.replace("POINT(", "");
    	wktPointAsString = wktPointAsString.replace(")", "");
    	
    	String[] xyAsStringArray = wktPointAsString.split(" ");
    	
    	result[0] = Double.parseDouble(xyAsStringArray[0].trim());
    	result[1] = Double.parseDouble(xyAsStringArray[1].trim());
    	
    	return result;
    }
    
    public static double[] getCoordinatesXYFromJSON(LinkedHashMap<?, ?> geometryMap) {

		double[] result = new double[2];

		Object coordinatesObject = geometryMap.get("coordinates");

		if (coordinatesObject instanceof List<?>) {
			List<?> coordinatesList = (List<?>) coordinatesObject;

			if (coordinatesList.size() > 1) {
				result[0] = (Double) coordinatesList.get(0);
				result[1] = (Double) coordinatesList.get(1);
			} else {
				LOGGER.error("Coordinates array is too small (must be 2), size is: "
						+ coordinatesList.size());
			}
		}

		return result;

	}
	
    public static Map<String, Object> getValuesFromFromJSON(Map<?, ?> phenomenonMap) {
		
		Map<String, Object> result = new HashMap<>();
		
		for (String propertyName : Properties.getPropertiesOfInterestDatatypeMapping().keySet()) {
			Object propertyObject = phenomenonMap.get(propertyName);
			
            if (propertyObject == null){
            	result.put(propertyName, 0.0);//TODO handle non number properties
			}else if(propertyObject instanceof LinkedHashMap<?, ?>){
				result.put(propertyName, ((LinkedHashMap<?, ?>)propertyObject).get("value"));
			} 
			
		}
				
		return result;
		
	}
    
    public static Map<?, ?> parseJsonStream(InputStream stream) throws IOException {
		ObjectMapper om = new ObjectMapper();
		final Map<?, ?> json = om.readValue(stream, Map.class);
		return json;
    }
	
}
