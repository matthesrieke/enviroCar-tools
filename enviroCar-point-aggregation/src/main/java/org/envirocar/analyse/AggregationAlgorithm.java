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
import org.envirocar.analyse.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

/**
 * Algorithm to aggregate measurements of tracks that are running through a defined bounding box.
 * 
 * @author Benjamin Pross
 *
 */
public class AggregationAlgorithm {

	private static final Logger LOGGER = LoggerFactory.getLogger(AggregationAlgorithm.class);
	
	private Geometry bbox;
	private double distance;
	private PointService pointService;
	private double maxx, maxy, minx, miny;
	
	public AggregationAlgorithm(double distance){
		pointService = new PostgresPointService(this);
		this.distance = distance;
	}
	
	public AggregationAlgorithm(double minx, double miny, double maxx, double maxy, double distance){
		
		this.maxx = maxx;
		this.maxy = maxy;
		this.minx = minx;
		this.miny = miny;
		
        Coordinate upperRight = new Coordinate(maxx, maxy);
        Coordinate upperLeft = new Coordinate(minx, maxy);
        Coordinate lowerRight = new Coordinate(maxx, miny);
        Coordinate lowerLeft = new Coordinate(minx, miny);
        
        Coordinate[] coordinates = new Coordinate[] {
                lowerLeft,
                lowerRight,
                upperRight,
                upperLeft,
                lowerLeft
            };
        
        bbox =  Utils.geometryFactory.createPolygon(coordinates);
		this.distance = distance;
		pointService = new PostgresPointService(this);
	}
	
	public void runAlgorithm(String trackID){
		
		LOGGER.debug("");
		LOGGER.debug("");
		LOGGER.debug("");
		LOGGER.debug("");
		
		LOGGER.debug("Track: " + trackID);
		LOGGER.debug("ResultSet size: " + pointService.getResultSet().size());
		
		LOGGER.debug("");
		LOGGER.debug("");
		LOGGER.debug("");
		LOGGER.debug("");		
		
		/*
		 * PointService get measurements for track 
		 */
		
		pointService.getMeasurementsOfTrack(trackID);
		
		/*
		 * Pointservice get next measurement
		 */

		Point nextPoint = pointService.getNextPoint(trackID);

		//TODO remove
		int count = 0;
		
		while (nextPoint != null) {
			
			/*
			 * check if point is fit for aggregation (one or more value not null or 0)
			 */
			if(!pointService.isFitForAggregation(nextPoint)){
				LOGGER.info("Skipping original point " + nextPoint.getID() + ". All values are null or 0.");
				nextPoint = pointService.getNextPoint(trackID);
				continue;
			}

			/*
			 * get nearest neighbor from resultSet
			 */
			
			Point nearestNeighbor = pointService.getNearestNeighbor(
					nextPoint.getID(), distance);

			List<Point> pointList = new ArrayList<>();
			
			pointList.add(nextPoint);
			
			if (nearestNeighbor != null) {
				
				/*
				 * check if point is fit for aggregation (one or more value not null or 0)
				 */
				if(!pointService.isFitForAggregation(nearestNeighbor)){
					LOGGER.info("Skipping result set point " + nearestNeighbor.getID() + ". All values are null or 0.");
					nextPoint = pointService.getNextPoint(trackID);
					continue;
				}
				
				pointList.add(nearestNeighbor);

				/*
				 * if there is one
				 * 
				 * aggregate values (avg, function should be
				 * replaceable)
				 */
				Point aggregatedPoint = pointService.aggregate(nextPoint, nearestNeighbor);

				pointList.add(aggregatedPoint);
				
				//TODO remove
//				try {
//					CSVExport.exportAsCSV(pointList, File.createTempFile(count + "-aggregation", ".csv").getAbsolutePath());
//				} catch (IOException e) {
//					LOGGER.error("Could not export resultSet as CSV:", e);
//				}
				/*
				 * PointService replace point in resultSet with aggregated
				 * point
				 */
				pointService.updateResultSet(nearestNeighbor.getID(),
						aggregatedPoint);

			} else {
				/*
				 * if there is no nearest neighbor
				 * 
				 * add point to resultSet
				 */					
				LOGGER.info("No nearest neighbor found for " + nextPoint.getID() + ". Adding to resultSet.");
				
				pointService.addToResultSet(nextPoint);

				//TODO remove
//				try {
//					CSVExport.exportAsCSV(pointList, File.createTempFile(count + "-aggregation", ".csv").getAbsolutePath());
//				} catch (IOException e) {
//					LOGGER.error("Could not export resultSet as CSV:", e);
//				}
			}
			
			count++;
			
			/*
			 * continue with next point in track
			 */
			nextPoint = pointService.getNextPoint(trackID);
		}
		
	}
	
	public void runAlgorithm(){
		
		/*
		 * get tracks
		 */
				
        List<String> trackIDs = getTrackIDs(minx, miny, maxx, maxy);
		
		/*
		 * foreach track
		 * 
		 */
        
		for (String trackID : trackIDs) {

			runAlgorithm(trackID);
			/* 
			 * continue with next track
			 */
		}
	}
	
	public List<String> getTrackIDs(double minx, double miny, double maxx, double maxy){
		
		List<String> result = new ArrayList<>();
        
		URL url = null;
		try {		
			url = new URL(Properties.getRequestTracksWithinBboxURL() + minx + "," + miny + "," + maxx + "," + maxy);
			
			LOGGER.debug("URL for fetching tracks: " + url.toString());
			
			InputStream in = url.openStream();

			ObjectMapper objMapper = new ObjectMapper();

			Map<?, ?> map = objMapper.readValue(in, Map.class);

			ArrayList<?> tracks = (ArrayList<?>) map.get("tracks");

			LOGGER.info("Number of tracks: " + tracks.size());
			
			for (Object object : tracks) {
				
				if(object instanceof LinkedHashMap<?, ?>){
					String id = String.valueOf(((LinkedHashMap<?, ?>)object).get("id"));
					
					result.add(id);
				}
			}
			
		} catch (MalformedURLException e) {
			LOGGER.error("URL seems to be malformed: " + url);
		} catch (IOException e) {
			LOGGER.error("Could not read from URL: " + url);
		}
		
		return result;
		
	}

	public Geometry getBbox() {
		return bbox;
	}

	public void setBbox(Geometry bbox) {
		this.bbox = bbox;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(double distance) {
		this.distance = distance;
	}
	
	public List<Point> getResultSet(){
		return pointService.getResultSet();
	}
	
}
