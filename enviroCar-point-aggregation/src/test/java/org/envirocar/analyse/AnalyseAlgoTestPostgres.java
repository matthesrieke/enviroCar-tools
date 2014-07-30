package org.envirocar.analyse;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
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

public class AnalyseAlgoTestPostgres {

	private static final Logger LOGGER = LoggerFactory.getLogger(AnalyseAlgoTestPostgres.class);
	
    private static Connection conn = null;
    private static String connectionURL = null;
	private static String databaseName = "postgres";
	private static String databasePath = "//192.168.136.133:5432";
	private static String username = "postgres";
	private static String password = "pOEdesoTL52oZg67yDFv";
	private static String aggregated_MeasurementsTableName = "aggregated_measurements";
	private static String spatial_ref_sysTableName = "spatial_ref_sys";
	private static String geometry_columnsTableName = "geometry_columns";
//	private static String urlString = "https://envirocar.org/api/stable/tracks?bbox=7.6224,51.94799,7.6539,51.96519";//about 100 tracks
	private static String urlString = "https://envirocar.org/api/stable/tracks?bbox=7.6224,51.94799,7.6339,51.95";//about six tracks
	private static String trackUrlString = "https://envirocar.org/api/stable/tracks/";
//	private List<String> propertiesOfInterest = Arrays.asList(new String[]{"CO2", "Speed"});
	private Map<String, Class<?>> propertiesOfInterestDatatypeMapping;
	private GeometryFactory geometryFactory;
	private Polygon bboxPolygon;
	private double distance;
	private String spatial_ref_sys = "4326";
	private String id_exp = "$id$";
	private String distance_exp = "$distance$";
	private PointService pointService;
	
	public AnalyseAlgoTestPostgres(){
	
	}
	
	public void init(){
	
		pointService = new PostgresPointService();
		
		propertiesOfInterestDatatypeMapping = new HashMap<>();
		
		propertiesOfInterestDatatypeMapping.put("CO2", Double.class);
		propertiesOfInterestDatatypeMapping.put("Speed", Double.class);
		
		createConnection();
//		queryNearestNeighbor("521cd817e4b00a043c4212b3", 0.1);
//		queryNearestNeighbor("521cd817e4b00a043c4212b5", 0.1);
		
		createTable(pgCreationString, aggregated_MeasurementsTableName);
////		createTable(pgGeometry_ColumnsCreationString, geometry_columnsTableName);
//		createTable(pgSpatial_Ref_SysCreationString, spatial_ref_sysTableName);
		executeStatement(addGeometryColumnToAggregated_MeasurementsTableString);
//		distance = 0.005; //5 meters
//		
		geometryFactory = new GeometryFactory();
////		
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
		
		queryNearestNeighbor("521cd817e4b00a043c4212b1", 0.1);
//		
//		runAlgorithm(trackPointMap);
	}
	
	private List<String> getTracks(Geometry bbox){
		
		List<String> result = new ArrayList<>();
        
		URL url;
		try {
			url = new URL(urlString);//TODO create URL from baseURL and bbox
			
			InputStream in = url.openStream();

			ObjectMapper objMapper = new ObjectMapper();

			Map<?, ?> map = objMapper.readValue(in, Map.class);

			ArrayList<?> tracks = (ArrayList<?>) map.get("tracks");

			LOGGER.info("Number of tracks: " + tracks.size());
			
			int count = 0;//TODO remove
			
			for (Object object : tracks) {
				
				if(count > 3){
					break;//TODO remove
				}
				
				if(object instanceof LinkedHashMap<?, ?>){
					String id = String.valueOf(((LinkedHashMap<?, ?>)object).get("id"));
					
					result.add(id);
				}
				count++;//TODO remove
			}
			
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
		
	}
	
	private void runAlgorithm(Map<String, List<Point>> trackPointMap){
		
		/*
		 * get tracks
		 * 
		 * pass trackIDs to PointService
		 * 
		 * PointService get Measurements for tracks
		 * 
		 * 
		 */
				
        List<String> trackIDs = getTracks(bboxPolygon);
		
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
					pointService.updateResultSet(nextPoint.getID(),
							nearestNeighbor);

				} else {
					/*
					 * if there is no nearest neighbor
					 * 
					 * add point to resultSet
					 */

					pointService.addToResultSet(nextPoint);
				}

				/*
				 * continue with next point in track
				 */
			}
			/* 
			 * continue with next track
			 */
		}
		
		exportAsCSV(pointService.getResultSet());
		
//		List<Point> resultSet = new ArrayList<>();
//		
//		Iterator<String> trackIDIterator = trackPointMap.keySet().iterator();
//		/*
//		 * loop over list of tracks
//		 */		
//		while (trackIDIterator.hasNext()) {
//			String id = (String) trackIDIterator.next();
//			
//			List<Point> points = trackPointMap.get(id);
//			
//			/*
//			 * loop over measurements of track
//			 */
//			for (Point point : points) {
//				
//				LOGGER.debug("Resultset size: " + resultSet.size());
//				
//				checkResultSet(point, resultSet, id);				
//			}
//			
//		}
//		
//		LOGGER.debug("Resultset size: " + resultSet.size());
//		LOGGER.debug("");
//		LOGGER.debug("##############################################################################################");
//		LOGGER.debug("#                                                                                            #");
//		LOGGER.debug("##############################################################################################");
//		LOGGER.debug("");
//		
//		for (Point point : resultSet) {
//			LOGGER.debug(point.getID() + " " + point.getNumberOfPointsUsedForAggregation());
//			for (String id : point.getTracksUsedForAggregation()) {
//				LOGGER.debug(id);
//			}
//			for (String propertyName : propertiesOfInterestDatatypeMapping.keySet()) {
//				LOGGER.debug(propertyName + ": " + point.getProperty(propertyName));
//			}
//		}
//				
//		exportAsCSV(resultSet);
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
		
		Iterator<String> propertyNameIterator = propertiesOfInterestDatatypeMapping.keySet().iterator();
				
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
		
		Iterator<String> propertyNameIterator = propertiesOfInterestDatatypeMapping.keySet().iterator();
		
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
		
		for (String propertyName : propertiesOfInterestDatatypeMapping.keySet()) {
			
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
						
						insertPoint(id, coordinatesXY[0], coordinatesXY[1], trackID, 1, propertiesofInterestMap);
						
//						Point newPoint = new InMemoryPoint(id, coordinatesXY[0], coordinatesXY[1], propertiesofInterestMap, trackID);
//						
//						points.add(newPoint);
					}
				}
			}
		}
    	
    	return points;
    	
	}
    
    private boolean insertPoint(String id, double x, double y, String trackID, int numberOfPoints, Map<String, Object> propertiesofInterestMap){
    	
    	String statement = createInsertPointStatement(id, x, y, trackID, numberOfPoints, propertiesofInterestMap);
    	
    	return executeInsertStatement(statement);
    }
    
    private String createInsertPointStatement(String id, double x, double y, String trackID, int numberOfPoints, Map<String, Object> propertiesofInterestMap){
    	
    	String columnNameString = "( id, the_geom, numberofpoints, numberofcontributingtracks, lastcontributingtrack, ";
    	String valueString = "( '" + id + "', ST_GeomFromText('POINT(" + x + " " + y + ")', " + spatial_ref_sys + "), " + numberOfPoints + ", " + 1 + ", '" + trackID + "', ";
    	
    	Iterator<String> propertyNameIterator = propertiesOfInterestDatatypeMapping.keySet().iterator();
    	
    	while (propertyNameIterator.hasNext()) {
    		String propertyName = (String) propertyNameIterator.next();
		
    		columnNameString = columnNameString.concat(propertyName.toLowerCase());
    		valueString = valueString.concat(String.valueOf(propertiesofInterestMap.get(propertyName)));
    		
    		if(propertyNameIterator.hasNext()){    			
        		columnNameString = columnNameString.concat(", ");
        		valueString = valueString.concat(", ");    			
    		}else{    			
        		columnNameString = columnNameString.concat(")");
        		valueString = valueString.concat(")");      			
    		}
    	}
    	
    	String statement = "INSERT INTO " + aggregated_MeasurementsTableName + columnNameString + "VALUES" + valueString + ";";
    	
    	return statement;
    }
    
    private Point queryNearestNeighbor(String id, double distance){
    	
    	String queryString = pgNearestNeighborCreationString.replace(id_exp, id).replace(distance_exp, "" + distance);
    	
    	ResultSet resultSet = executeQueryStatement(queryString);
    	
		try {

			if (resultSet != null) {

				while (resultSet.next()) {

					String resultID = resultSet.getString("id");
					
					Map<String, Object> propertyMap = new HashMap<>();
					
					for (String propertyName : propertiesOfInterestDatatypeMapping.keySet()) {
						
						Class<?> propertyClass = propertiesOfInterestDatatypeMapping.get(propertyName);
						
						Object value = null;
						
						if(propertyClass.equals(Double.class)){
							value = resultSet.getDouble(propertyName.toLowerCase());
						}
						
						propertyMap.put(propertyName, value);
					}
					
					String resultLastContributingTrack = resultSet.getString("lastcontributingtrack");
					
					int resultNumberOfContributingPoints = resultSet.getInt("numberofpoints");
					
					int resultNumberOfContributingTracks = resultSet.getInt("numberofcontributingtracks");
					
					String resultGeomAsText = resultSet.getString("text_geom");
					
					double[] resultXY = convertWKTPointToXY(resultGeomAsText);
					
					LOGGER.debug(resultSet.getString("id"));
					LOGGER.debug("" + resultSet.getDouble("dist"));
					LOGGER.debug(resultSet.getString("text_geom"));
					
					Point resultPoint = new InMemoryPoint(resultID, resultXY[0], resultXY[1], propertyMap, resultNumberOfContributingPoints, resultNumberOfContributingTracks, resultLastContributingTrack);
					
					return resultPoint;
				}

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
    	
		return new InMemoryPoint();
    }
    
    public double[] convertWKTPointToXY(String wktPointAsString){
    	
    	double[] result = new double[2];
    	
    	wktPointAsString = wktPointAsString.replace("POINT(", "");
    	wktPointAsString = wktPointAsString.replace(")", "");
    	
    	String[] xyAsStringArray = wktPointAsString.split(" ");
    	
    	result[0] = Double.parseDouble(xyAsStringArray[0].trim());
    	result[1] = Double.parseDouble(xyAsStringArray[1].trim());
    	
    	return result;
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
		
		for (String propertyName : propertiesOfInterestDatatypeMapping.keySet()) {
			Object propertyObject = phenomenonMap.get(propertyName);
			
			if(propertyObject instanceof LinkedHashMap<?, ?>){
				result.put(propertyName, ((LinkedHashMap<?, ?>)propertyObject).get("value"));
			}
			
		}
				
		return result;
		
	}

	public static final String pgCreationString = "CREATE TABLE " + aggregated_MeasurementsTableName +  " ("
            + "ID VARCHAR(24) NOT NULL PRIMARY KEY, "
            + "NUMBEROFPOINTS INTEGER, "
            + "NUMBEROFCONTRIBUTINGTRACKS INTEGER,"
            + "LASTCONTRIBUTINGTRACK VARCHAR(24),"
            + "CO2 DOUBLE PRECISION,"
            + "SPEED DOUBLE PRECISION)";
	
	public static final String pgGeometry_ColumnsCreationString = "CREATE TABLE " + geometry_columnsTableName +  " "
			+ " ( "
			+ " f_table_catalog character varying(256) NOT NULL, "
			+ " f_table_schema character varying(256) NOT NULL, "
			+ " f_table_name character varying(256) NOT NULL, "
			+ " f_geometry_column character varying(256) NOT NULL, "
			+ " coord_dimension integer NOT NULL, "
			+ " srid integer NOT NULL, "
			+ " type character varying(30) NOT NULL, "
			+ " CONSTRAINT geometry_columns_pk PRIMARY KEY (f_table_catalog, f_table_schema, f_table_name, f_geometry_column) "
			+ " ) "
			+ " WITH ( "
			+ "   OIDS=TRUE "
			+ " ); "
			+ " ALTER TABLE geometry_columns " + "   OWNER TO postgres;";
	
	public static final String pgSpatial_Ref_SysCreationString = "CREATE TABLE " + spatial_ref_sysTableName +  " "
			+ " ( "
			+ " srid integer NOT NULL, "
			+ " auth_name character varying(256), "
			+ " auth_srid integer, "
			+ " srtext character varying(2048), "
			+ " proj4text character varying(2048), "
			+ " CONSTRAINT spatial_ref_sys_pkey PRIMARY KEY (srid) "
			+ " ) "
			+ " WITH ( "
			+ "   OIDS=FALSE "
			+ " );"
			+ " ALTER TABLE geometry_columns " + "   OWNER TO postgres;";
	
	public final String pgNearestNeighborCreationString = "select h.id, h.speed, h.co2, h.numberofpoints, h.numberofcontributingtracks, h.lastcontributingtrack, ST_AsText(h.the_geom) as text_geom, ST_distance(w.the_geom,h.the_geom) as dist from aggregated_measurements h, "
			+ "(select * from aggregated_measurements where id='" + id_exp + "') w "
			+ "where ST_DWithin(w.the_geom,h.the_geom," + distance_exp + ") AND NOT w.id = h.id " + "order by dist";
	
	public static final String addGeometryColumnToAggregated_MeasurementsTableString = "SELECT AddGeometryColumn( '" + aggregated_MeasurementsTableName.toLowerCase() +  "', 'the_geom', 4326, 'POINT', 2 );";
    
    public static void main(String[] args) {
    	new AnalyseAlgoTestPostgres().init();
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
    
	private boolean executeStatement(String statement) {
		try {
			Statement st = conn.createStatement();
			st.execute(statement);

			conn.commit();

		} catch (SQLException e) {
			LOGGER.error("Execution of the following statement failed: " + statement + " cause: "
					+ e.getMessage());
			return false;
		}
		return true;
	}
	
	private ResultSet executeQueryStatement(String statement) {
		try {
			Statement st = conn.createStatement();
			ResultSet resultSet = st.executeQuery(statement);
			
			conn.commit();
			
			return resultSet;
			
		} catch (SQLException e) {
			LOGGER.error("Execution of the following statement failed: " + statement + " cause: "
					+ e.getMessage());
			return null;
		}
	}
	
	private boolean executeInsertStatement(String statement) {
		try {
			Statement st = conn.createStatement();
			st.executeUpdate(statement);
			
			conn.commit();
			
		} catch (SQLException e) {
			LOGGER.error("Execution of the following statement failed: " + statement + " cause: "
					+ e.getMessage());
			return false;
		}
		return true;
	}

    private static boolean createTable(String creationString, String tableName) {
        try {
            ResultSet rs;
            DatabaseMetaData meta = conn.getMetaData();
            rs = meta.getTables(null, null, tableName, new String[]{"TABLE"});
            if (!rs.next()) {
                LOGGER.info("Table " + tableName + " does not yet exist.");
                Statement st = conn.createStatement();
                st.executeUpdate(creationString);

                conn.commit();

                meta = conn.getMetaData();

                rs = meta.getTables(null, null, tableName, new String[]{"TABLE"});
                if (rs.next()) {
                    LOGGER.info("Succesfully created table " + tableName + ".");
                } else {
                    LOGGER.error("Could not create table " + tableName + ".");
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
