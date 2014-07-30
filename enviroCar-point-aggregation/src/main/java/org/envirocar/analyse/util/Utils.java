package org.envirocar.analyse.util;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.envirocar.analyse.properties.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
				result[0] = (double) coordinatesList.get(0);
				result[1] = (double) coordinatesList.get(1);
			} else {
				LOGGER.error("Coordinates array is too small (must be 2), size is: "
						+ coordinatesList.size());
			}
		}

		return result;

	}
	
    public static Map<String, Object> getValuesFromFromJSON(LinkedHashMap<?, ?> phenomenonMap) {
		
		Map<String, Object> result = new HashMap<>();
		
		for (String propertyName : Properties.getPropertiesOfInterestDatatypeMapping().keySet()) {
			Object propertyObject = phenomenonMap.get(propertyName);
			
			if(propertyObject instanceof LinkedHashMap<?, ?>){
				result.put(propertyName, ((LinkedHashMap<?, ?>)propertyObject).get("value"));
			}
			
		}
				
		return result;
		
	}
	
}
