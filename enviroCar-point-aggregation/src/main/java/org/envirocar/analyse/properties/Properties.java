package org.envirocar.analyse.properties;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.envirocar.analyse.PostgresPointService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Properties {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(Properties.class);
	
    private static final String PROPERTIES = "/aggregation.properties";
    private static final String DEFAULT_PROPERTIES = "/aggregation.default.properties";
	private static java.util.Properties properties;
	
	private static Map<String, Class<?>> propertiesOfInterestDatatypeMapping;
	
	private static String baseURL;
	
	private static String requestTrackURL;

	private static String requestTracksWithinBboxURL;
	
	public static Object getProperty(String propertyName){
		return getProperties().getProperty(propertyName);
	}
	
	public static java.util.Properties getProperties() {
		
		if(properties == null){
			
			properties = new java.util.Properties();
	        InputStream in = null;
	        try {
	            in = PostgresPointService.class.getResourceAsStream(PROPERTIES);
	            if (in != null) {
	                properties.load(in);
	            } else {
	            	LOGGER.info("No {} found, loading {}.", PROPERTIES, DEFAULT_PROPERTIES);
	                in = PostgresPointService.class.getResourceAsStream(DEFAULT_PROPERTIES);
	                if (in != null) {
	                    properties.load(in);
	                }else{
	                	LOGGER.warn("No {} found!", PROPERTIES);
	                }
	            }	            
	        } catch (IOException ex) {
	            LOGGER.error("Error reading " + PROPERTIES, ex);
	        } finally {
	            try {
					in.close();
				} catch (IOException e) {}
	        }
			
		}
		
		return properties;
	}
	
	public static String getBaseURL() {
		
		if(baseURL == null || baseURL.equals("")){
			baseURL = getProperties().getProperty("baseURL");
		}
		
		return baseURL;
	}

	public static String getRequestTrackURL() {
		return getBaseURL() + "tracks/";
	}

	public static String getRequestTracksWithinBboxURL() {
		return getBaseURL() + "tracks?bbox=";
	}
	
	//TODO make configurable
	public static Map<String, Class<?>> getPropertiesOfInterestDatatypeMapping(){
		
		if(propertiesOfInterestDatatypeMapping == null){
			propertiesOfInterestDatatypeMapping = new HashMap<>();
			
			propertiesOfInterestDatatypeMapping.put("CO2", Double.class);
			propertiesOfInterestDatatypeMapping.put("Speed", Double.class);
		}
		return propertiesOfInterestDatatypeMapping;
	}
	
}
