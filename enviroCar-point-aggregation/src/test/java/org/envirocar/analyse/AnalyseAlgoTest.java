package org.envirocar.analyse;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.bson.types.ObjectId;
import org.envirocar.analyse.entities.InMemoryPoint;
import org.envirocar.analyse.entities.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;

public class AnalyseAlgoTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(AnalyseAlgoTest.class);
	
    private static Connection conn = null;
    private static String connectionURL = null;
	private static String databaseName = "postgres";
	private static String databasePath = "//192.168.136.133:5432";
	private static String username = "postgres";
	private static String password = "pOEdesoTL52oZg67yDFv";
	private static String measurementsTableName = "MEASUREMENTS";
//	private static String urlString = "https://envirocar.org/api/stable/tracks?bbox=7.6224,51.94799,7.6539,51.96519";//about 100 tracks
	private static String urlString = "https://envirocar.org/api/stable/tracks?bbox=7.6224,51.94799,7.6339,51.95";//about six tracks
	private static String trackUrlString = "https://envirocar.org/api/stable/tracks/";
	private List<String> propertiesOfInterest = Arrays.asList(new String[]{"CO2", "Speed"});
	private GeometryFactory geometryFactory;
	private Polygon bboxPolygon;
	private double distance;
	
	public AnalyseAlgoTest(){
//		createConnection();
//		createResultTable();
		
		distance = 0.005; //5 meters
		
		geometryFactory = new GeometryFactory();
//		
		double maxx = 7.6539;
		double maxy = 51.96519;
		double minx = 7.6224;
		double miny = 51.94799;
		
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
        
        bboxPolygon =  geometryFactory.createPolygon(coordinates);
				
        Map<String, List<Point>> trackPointMap = new HashMap<>();
        
		URL url;
		try {
			url = new URL(urlString);
			
			InputStream in = url.openStream();

			ObjectMapper objMapper = new ObjectMapper();

			Map<?, ?> map = objMapper.readValue(in, Map.class);

			ArrayList<?> tracks = (ArrayList<?>) map.get("tracks");

			LOGGER.info("Number of tracks: " + tracks.size());
			
			int count = 0;
			
			for (Object object : tracks) {
				
				if(count > 3){
					break;
				}
				
				if(object instanceof LinkedHashMap<?, ?>){
					String id = String.valueOf(((LinkedHashMap<?, ?>)object).get("id"));
					
					trackPointMap.put(id, createPointsForTrack(id));
				}
				count++;
			}
			
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		LOGGER.info("Size of trackPointMap: " + trackPointMap.size());
		
		runAlgorithm(trackPointMap);
	}
	
	private void runAlgorithm(Map<String, List<Point>> trackPointMap){
		
		List<Point> resultSet = new ArrayList<>();
		
		Iterator<String> trackIDIterator = trackPointMap.keySet().iterator();
		/*
		 * loop over list of tracks
		 */		
		while (trackIDIterator.hasNext()) {
			String id = (String) trackIDIterator.next();
			
			List<Point> points = trackPointMap.get(id);
			
			/*
			 * loop over measurements of track
			 */
			for (Point point : points) {
				
				LOGGER.debug("Resultset size: " + resultSet.size());
				
				checkResultSet(point, resultSet, id);				
			}
			
		}
		
		LOGGER.debug("Resultset size: " + resultSet.size());
		LOGGER.debug("");
		LOGGER.debug("##############################################################################################");
		LOGGER.debug("#                                                                                            #");
		LOGGER.debug("##############################################################################################");
		LOGGER.debug("");
		
		for (Point point : resultSet) {
			LOGGER.debug(point.getID() + " " + point.getNumberOfPointsUsedForAggregation());
			for (String id : point.getTracksUsedForAggregation()) {
				LOGGER.debug(id);
			}
			for (String propertyName : propertiesOfInterest) {
				LOGGER.debug(propertyName + ": " + point.getProperty(propertyName));
			}
		}
				
		exportAsCSV(resultSet);
	}
	
	private void exportAsCSV(List<Point> resultSet) {
		File csvFile = new File("d:/tmp/result2.csv");
		
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

	private String createCSVHeader() {
		
		String result = "ID;lon;lat;";
		
		Iterator<String> propertyNameIterator = propertiesOfInterest.iterator();
				
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

	private String createCSVLine(Point point){
		
		String result = point.getID() + ";" + point.getX() + ";" + point.getY() + ";";
		
		Iterator<String> propertyNameIterator = propertiesOfInterest.iterator();
		
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
	
	private void checkResultSet(Point point, List<Point> resultSet, String trackID) {
		
		Point closestPointInRange = getClosestPointsInRage(point, resultSet);
		
		if(closestPointInRange == null){
			LOGGER.debug("Did not find a point in range of " + distance + " km.");
			resultSet.add(point);
			return;
		}
		
		updateValues(point, closestPointInRange);
		
		point.setNumberOfPointsUsedForAggregation(point.getNumberOfPointsUsedForAggregation() + 1);
		
		if(!point.getTracksUsedForAggregation().contains(trackID)){		
			point.addTrackUsedForAggregation(trackID);
		}
		
		/*
		 * exchange point within the result set with the result of the aggregation
		 */
		ObjectId id = new ObjectId();
		point.setID(id.toString());
		resultSet.remove(closestPointInRange);
		resultSet.add(point);
		
//		double weightedAvg = getWeightedAverage(point, closestPointInRange, "CO2");
		
//		LOGGER.debug("Weighted average: " + weightedAvg);
		
	}
	
	private void updateValues(Point source, Point closestPointInRange){
		
		for (String propertyName : propertiesOfInterest) {
			
			double weightedAvg = getWeightedAverage(source, closestPointInRange, propertyName);
			
			LOGGER.debug("Weighted average: " + weightedAvg);
			
			source.setProperty(propertyName, weightedAvg);
		}
		
	}
	
	private double getWeightedAverage(Point source, Point point,
			String propertyName) {

		Object sourceNumberObject = source.getProperty(propertyName);

		if (sourceNumberObject instanceof Number) {

			Number sourceValue = (Number) sourceNumberObject;

			double summedUpValues = sourceValue.doubleValue();

			Object pointNumberObject = point.getProperty(propertyName);
			if (pointNumberObject instanceof Number) {
				summedUpValues = summedUpValues
						+ ((Number) pointNumberObject).doubleValue();
			}

			return summedUpValues / 2;
		}

		LOGGER.debug("source property not a number");
		
		return -1;
	}
	
	private Point getClosestPointsInRage(Point startPoint, List<Point> points) {

		Point result = null;
		double closestDistance = Double.MAX_VALUE;
		/*
		 * These points will be added to the resultset
		 */	
//		List<Point> pointsNotInRange = new ArrayList<>();
		
		for (Point point : points) {
			
			double distance = isInRange(startPoint, point);
			
			if (distance != -1 && (distance < closestDistance)) {
				result = point;
				closestDistance = distance;
			}
//			else{
//				
//				LOGGER.debug("Point with id " + point.getID() + " not in range of point with id " + startPoint.getID() + ". Adding to resultset.");
//				
//				//add to resultset
//				pointsNotInRange.add(point);
//			}
		}

//		points.addAll(pointsNotInRange);
		
		return result;
		
	}

	private List<Point> getPointsInRage(Point startPoint, Map<String, List<Point>> trackPointMap){
		
		List<Point> result = new ArrayList<>();
		
		for (String id : trackPointMap.keySet()) {
			List<Point> points = trackPointMap.get(id);
			
			for (Point point : points) {
				
				if(isInRange(startPoint, point) != -1){
					result.add(point);
				}
				
			}			
		}
		
		return result;
		
	}
	
	private double isInRange(Point source, Point target){
		
		double distance = getDistance(source.getX(), source.getY(), target.getX(), target.getY());
		
		if(distance <= this.distance){		
			LOGGER.debug("Distance between " + source.getID() + " and " + target.getID() + " = " + distance);		
		}
		
		return (distance <= this.distance) ? distance : -1;
		
	}
	
	/**
	 * Returns the distance of two points in kilometers.
	 * 
	 * @param lat1
	 * @param lng1
	 * @param lat2
	 * @param lng2
	 * @return distance in km
	 */
	public double getDistance(double lat1, double lng1, double lat2, double lng2) {

		double earthRadius = 6369;
		double dLat = Math.toRadians(lat2 - lat1);
		double dLng = Math.toRadians(lng2 - lng1);
		double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		double dist = earthRadius * c;

		return dist;

	}
	
	private List<Point> createPointsForTrack(String id){
		
		URL url;
		try {
			url = new URL(trackUrlString + id);
			
			InputStream in = url.openStream();

			ObjectMapper objMapper = new ObjectMapper();

			Map<?, ?> map = objMapper.readValue(in, Map.class);

			ArrayList<?> features = null;

			for (Object o : map.keySet()) {
				Object entry = map.get(o);

				if (o.equals("features")) {
					features = (ArrayList<?>) entry;
				}
			}
			
			return createPointsFromJSON(features, id, bboxPolygon);
			
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return Collections.emptyList();
		
	}
	
    private List<Point> createPointsFromJSON(ArrayList<?> features, String trackID, Geometry bbox) {
		
    	List<Point> points = new ArrayList<>();
    	    	
    	for (Object object : features) {

			if (object instanceof LinkedHashMap<?, ?>) {
				LinkedHashMap<?, ?> featureMap = (LinkedHashMap<?, ?>) object;

				Object geometryObject = featureMap.get("geometry");
				
				double[] coordinatesXY = new double[2];
				
				if (geometryObject instanceof LinkedHashMap<?, ?>) {
					coordinatesXY = getCoordinatesXYFromJSON((LinkedHashMap<?, ?>) geometryObject);
				}
				
				Coordinate pointCoordinate = new Coordinate(coordinatesXY[0], coordinatesXY[1]);
				
				if(!bbox.contains(geometryFactory.createPoint(pointCoordinate))){
					continue;
				}
				
				Object propertiesObject = featureMap.get("properties");				
				
				if (propertiesObject instanceof LinkedHashMap<?, ?>) {
					LinkedHashMap<?, ?> propertiesMap = (LinkedHashMap<?, ?>) propertiesObject;

					String id = String.valueOf(propertiesMap.get("id"));
					
					Object phenomenonsObject = propertiesMap.get("phenomenons");

					if (phenomenonsObject instanceof LinkedHashMap<?, ?>) {
						LinkedHashMap<?, ?> phenomenonsMap = (LinkedHashMap<?, ?>) phenomenonsObject;

						Map<String, Object> propertiesofInterestMap = getValuesFromFromJSON(phenomenonsMap);
						
						Point newPoint = new InMemoryPoint(id, coordinatesXY[0], coordinatesXY[1], propertiesofInterestMap, trackID);
						
						points.add(newPoint);
					}
				}
			}
		}
    	
    	return points;
    	
	}
    
	private double[] getCoordinatesXYFromJSON(LinkedHashMap<?, ?> geometryMap) {

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
	
	private Map<String, Object> getValuesFromFromJSON(LinkedHashMap<?, ?> phenomenonMap) {
		
		Map<String, Object> result = new HashMap<>();
		
		for (String propertyName : propertiesOfInterest) {
			Object propertyObject = phenomenonMap.get(propertyName);
			
			if(propertyObject instanceof LinkedHashMap<?, ?>){
				result.put(propertyName, ((LinkedHashMap<?, ?>)propertyObject).get("value"));
			}
			
		}
				
		return result;
		
	}

	public static final String pgCreationString = "CREATE TABLE " + measurementsTableName +  " ("
            + "REQUEST_ID VARCHAR(100) NOT NULL PRIMARY KEY, "
            + "REQUEST_DATE TIMESTAMP, "
            + "RESPONSE_TYPE VARCHAR(100), "
            + "RESPONSE TEXT, "
            + "RESPONSE_MIMETYPE VARCHAR(100))";
    
    public static void main(String[] args) {
    	new AnalyseAlgoTest();
	}
    
    private static boolean createConnection() {
        
    	connectionURL = "jdbc:postgresql:" + getDatabasePath() + "/" + getDatabaseName();
    	
    	Properties props = new Properties();
    	
            props.setProperty("create", "true");
            props.setProperty("user", username);
            props.setProperty("password", password);
            conn = null;
            try {
                conn = DriverManager.getConnection(
                        connectionURL, props);
                conn.setAutoCommit(false);
                LOGGER.info("Connected to WPS database.");
            } catch (SQLException e) {
                LOGGER.error("Could not connect to or create the database.", e);
                return false;
            }
        
        return true;
    }

    private static boolean createResultTable() {
        try {
            ResultSet rs;
            DatabaseMetaData meta = conn.getMetaData();
            rs = meta.getTables(null, null, measurementsTableName.toLowerCase(), new String[]{"TABLE"});
            if (!rs.next()) {
                LOGGER.info("Table " + measurementsTableName + " does not yet exist.");
                Statement st = conn.createStatement();
                st.executeUpdate(pgCreationString);

                conn.commit();

                meta = conn.getMetaData();

                rs = meta.getTables(null, null, measurementsTableName, new String[]{"TABLE"});
                if (rs.next()) {
                    LOGGER.info("Succesfully created table " + measurementsTableName + ".");
                } else {
                    LOGGER.error("Could not create table " + measurementsTableName + ".");
                    return false;
                }
            }
        } catch (SQLException e) {
            LOGGER.error("Connection to the Postgres database failed: " + e.getMessage());
            return false;
        }
        return true;
    }

	private static String getDatabaseName() {
		return databaseName;
	}

	private static String getDatabasePath() {
		return databasePath;
	}
	
	private Set<String> gatherPropertiesForFeatureTypeBuilder(ArrayList<?> features) {
		Set<String> distinctPhenomenonNames = new HashSet<String>();

		for (Object object : features) {

			if (object instanceof LinkedHashMap<?, ?>) {
				LinkedHashMap<?, ?> featureMap = (LinkedHashMap<?, ?>) object;

				Object propertiesObject = featureMap.get("properties");

				if (propertiesObject instanceof LinkedHashMap<?, ?>) {
					LinkedHashMap<?, ?> propertiesMap = (LinkedHashMap<?, ?>) propertiesObject;

					Object phenomenonsObject = propertiesMap.get("phenomenons");

					if (phenomenonsObject instanceof LinkedHashMap<?, ?>) {
						LinkedHashMap<?, ?> phenomenonsMap = (LinkedHashMap<?, ?>) phenomenonsObject;

						for (Object phenomenonKey : phenomenonsMap.keySet()) {

							Object phenomenonValue = phenomenonsMap
									.get(phenomenonKey);

							if (phenomenonValue instanceof LinkedHashMap<?, ?>) {
								LinkedHashMap<?, ?> phenomenonValueMap = (LinkedHashMap<?, ?>) phenomenonValue;

								String unit = phenomenonValueMap.get("unit")
										.toString();

								distinctPhenomenonNames.add(phenomenonKey
										.toString() + " (" + unit + ")");
							}

						}
					}

				}
			}
		}
		return distinctPhenomenonNames;
	}	
	
}
