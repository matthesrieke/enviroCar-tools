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
	
	public static String getProperty(String propertyName){
		return getProperties().getProperty(propertyName);
	}
	
	public synchronized static java.util.Properties getProperties() {
		
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
		
		if(requestTrackURL == null || requestTrackURL.equals("")){
			requestTrackURL = getBaseURL() + "tracks/";
		}
		
		return requestTrackURL;
	}

	public static String getRequestTracksWithinBboxURL() {
		
		if(requestTracksWithinBboxURL == null || requestTracksWithinBboxURL.equals("")){
			requestTracksWithinBboxURL = getBaseURL() + "tracks?bbox=";
		}
		
		return requestTracksWithinBboxURL;
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
