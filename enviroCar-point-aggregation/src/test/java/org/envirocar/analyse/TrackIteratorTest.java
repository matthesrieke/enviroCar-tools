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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.envirocar.analyse.entities.Point;
import org.envirocar.analyse.properties.Properties;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TrackIteratorTest {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(TrackIteratorTest.class);
	
	@Test
	public void testAlgorithm(){
        
		PointService pointService = new PostgresPointService();
		
		String trackID = "53b5228ee4b01607fa566b78";
		
		List<String> measurementIDs = getMeasurementIDsOfTrack(trackID);
		
		pointService.getMeasurementsOfTrack(trackID);
		
		Point point = null;
		
		while ((point = pointService.getNextPoint(trackID)) != null) {
			
			String pointID = point.getID();
			
			assertTrue(measurementIDs.remove(pointID));
			
			LOGGER.info(pointID);			
		}		
		
		assertTrue(measurementIDs.isEmpty());
	}
	
	public List<String> getMeasurementIDsOfTrack(String trackID) {
		
		URL url = null;
		try {
			url = new URL(Properties.getRequestTrackURL() + trackID);
			
			InputStream in = url.openStream();

			ObjectMapper objMapper = new ObjectMapper();

			Map<?, ?> map = objMapper.readValue(in, Map.class);

			ArrayList<?> features = null;

			for (Object o : map.keySet()) {
				Object entry = map.get(o);

				if (o.equals("features")) {
					features = (ArrayList<?>) entry;
					break;
				}
			}
			
			return extractMeasurementIDsFromJSON(features);
			
		} catch (MalformedURLException e) {
			LOGGER.error("Malformed URL: " + url == null ? null : url.toString(), e);
		} catch (IOException e) {
			LOGGER.error("Error getting measurements of track:" + trackID, e);
		}
		return new ArrayList<>();
	}
	
	
    private List<String> extractMeasurementIDsFromJSON(ArrayList<?> features) {
    	
    	List<String> result  = new ArrayList<>();
    	
    	for (Object object : features) {

			if (object instanceof LinkedHashMap<?, ?>) {
				LinkedHashMap<?, ?> featureMap = (LinkedHashMap<?, ?>) object;
				
				Object propertiesObject = featureMap.get("properties");				
				
				if (propertiesObject instanceof LinkedHashMap<?, ?>) {
					LinkedHashMap<?, ?> propertiesMap = (LinkedHashMap<?, ?>) propertiesObject;

					String id = String.valueOf(propertiesMap.get("id"));
					
					result.add(id);
				}
			}
		}
    	
    	return result;
    	
	}
	
}
