package org.envirocar.analyse;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.envirocar.analyse.entities.Point;
import org.envirocar.analyse.export.csv.CSVExport;
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
	
	public AggregationAlgorithm(){
		pointService = new PostgresPointService(this);
	}
	
	public AggregationAlgorithm(double minx, double miny, double maxx, double maxy, double distance){
		
		this.maxx = maxx;
		this.maxy = maxy;
		this.minx = minx;
		this.miny = miny;
		
		/*
		 * TODO remove this
		 */
		
		double maxx2 = 7.6539;
		double maxy2 = 51.96519;
		double minx2 = 7.6224;
		double miny2 = 51.94799;
		
		/*
		 * TODO remove
		 */
		
		Coordinate upperRight = new Coordinate(maxx2, maxy2);
        Coordinate upperLeft = new Coordinate(minx2, maxy2);
        Coordinate lowerRight = new Coordinate(maxx2, miny2);
        Coordinate lowerLeft = new Coordinate(minx2, miny2);
		
//        Coordinate upperRight = new Coordinate(maxx, maxy);
//        Coordinate upperLeft = new Coordinate(minx, maxy);
//        Coordinate lowerRight = new Coordinate(maxx, miny);
//        Coordinate lowerLeft = new Coordinate(minx, miny);
        
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
	
	public void runAlgorithm(){
		
		/*
		 * get tracks
		 * 
		 * pass trackIDs to PointService
		 * 
		 * PointService get Measurements for tracks
		 * 
		 * 
		 */
				
        List<String> trackIDs = getTracks(bbox);
		
        pointService.getMeasurementsOfTracks(trackIDs);
        
        
		/*
		 * foreach track
		 * 
		 */
        
		for (String trackID : trackIDs) {

			/*
			 * Pointservice get next measurement
			 */

			Point nextPoint = pointService.getNextPoint(trackID);

			while (nextPoint != null) {

				/*
				 * get nearest neighbor from resultSet
				 */

				Point nearestNeighbor = pointService.getNearestNeighbor(
						nextPoint.getID(), distance);

				if (nearestNeighbor != null) {

					/*
					 * if there is one
					 * 
					 * aggregate values (weighted avg, function should be
					 * replaceable)
					 */
					pointService.aggregate(nextPoint, nearestNeighbor);
					/*
					 * PointService replace point in resultSet with aggregated
					 * point
					 */
					pointService.updateResultSet(nearestNeighbor.getID(),
							nextPoint);

				} else {
					/*
					 * if there is no nearest neighbor
					 * 
					 * add point to resultSet
					 */					
					LOGGER.info("No nearest neighbor found for " + nextPoint.getID() + ". Adding to resultSet.");
					
					pointService.addToResultSet(nextPoint);
				}

				/*
				 * continue with next point in track
				 */
				nextPoint = pointService.getNextPoint(trackID);
			}
			/* 
			 * continue with next track
			 */
		}
		
		try {
			CSVExport.exportAsCSV(pointService.getResultSet(), File.createTempFile("aggregation", ".csv").getAbsolutePath());
		} catch (IOException e) {
			LOGGER.error("Could not export resultSet as CSV:", e);
		}
	}
	
	private List<String> getTracks(Geometry bbox){
		
		List<String> result = new ArrayList<>();
        
		URL url = null;
		try {		
			url = new URL(Properties.requestTracksWithinBboxURL + minx + "," + miny + "," + maxx + "," + maxy);
			
			InputStream in = url.openStream();

			ObjectMapper objMapper = new ObjectMapper();

			Map<?, ?> map = objMapper.readValue(in, Map.class);

			ArrayList<?> tracks = (ArrayList<?>) map.get("tracks");

			LOGGER.info("Number of tracks: " + tracks.size());
			
//			int count = 0;//TODO remove
			
			for (Object object : tracks) {
				
//				if(count > 3){
//					break;//TODO remove
//				}
				
				if(object instanceof LinkedHashMap<?, ?>){
					String id = String.valueOf(((LinkedHashMap<?, ?>)object).get("id"));
					
					result.add(id);
				}
//				count++;//TODO remove
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
}
