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
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;
import org.envirocar.analyse.entities.InMemoryPoint;
import org.envirocar.analyse.entities.Point;
import org.envirocar.analyse.properties.Properties;
import org.envirocar.analyse.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class PostgresPointService implements PointService {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(PostgresPointService.class);

	private Connection conn = null;
	private String connectionURL = null;
	private String databaseName;
	private String databasePath;
															
	private String username;
	private String password;
	private String aggregated_MeasurementsTableName = "aggregated_measurements";// TODO
	private String original_MeasurementsTableName = "original_measurements";// TODO from
																				// properties
	private String measurementRelationsTableName = "measurement_relations";// TODO from properties
	
	private String spatial_ref_sys = "4326";// TODO from properties
	private String id_exp = "$id$";
	private String distance_exp = "$distance$";
	private String table_name_exp = "$tablename$";
	private String geomFromText_exp = "$geom_from_text$";
	private AggregationAlgorithm algorithm;
	private Map<String, ResultSet> trackDatabasePointsResultSetMap;
	private Map<String, List<Point>> trackMeasurementsAsPointsMap;

	private String idField = "id";
	private String generalNumberOfContributingPointsField = "generalnumberofcontributingpoints";
	private String generalnumberOfContributingTracksField = "generalnumberofcontributingtracks";
	private String lastContributingTrackField = "lastcontributingtrack";
	private String co2Field = "co2";
	private String speedField = "speed";
	private String co2NumberOfContributingPointsField = "co2numberofcontributingpoints";
	private String speedNumberOfContributingPointsField = "speednumberofcontributingpoints";
	private String trackIDField = "trackid";
	private String geometryEncodedField = "the_geom";
	private String geometryPlainTextField = "text_geom";
	private String distField = "dist";
	
	public final String pgCreationString = "CREATE TABLE "
			+ aggregated_MeasurementsTableName + " ("
			+ idField + " VARCHAR(24) NOT NULL PRIMARY KEY, "
			+ generalNumberOfContributingPointsField + " INTEGER, "
			+ generalnumberOfContributingTracksField + " INTEGER,"
			+ lastContributingTrackField + " VARCHAR(24)," 
			+ co2Field + " DOUBLE PRECISION," 
			+ co2NumberOfContributingPointsField + " INTEGER, "
			+ speedField + " DOUBLE PRECISION, " 
			+ speedNumberOfContributingPointsField + " INTEGER)";
	
	public final String pgOriginalMeasurementsTableCreationString = "CREATE TABLE "
			+ original_MeasurementsTableName + " ("
			+ "ID VARCHAR(24) NOT NULL PRIMARY KEY, "
			+ "TRACKID VARCHAR(24)," + "CO2 DOUBLE PRECISION,"
			+ "SPEED DOUBLE PRECISION)";
	
	public final String pgMeasurementRelationsTableCreationString = "CREATE TABLE "
			+ measurementRelationsTableName + " ("
			+ "id VARCHAR(24) NOT NULL PRIMARY KEY, "
			+ "aggregated_measurement_id VARCHAR(24))";

	public String pgNearestNeighborCreationString = "select h." + idField + ", h." + speedField + ", h." + co2Field + ", h." + generalNumberOfContributingPointsField + ", h." + speedNumberOfContributingPointsField + ", h." + co2NumberOfContributingPointsField + ", h." + generalnumberOfContributingTracksField + ", h." + lastContributingTrackField + ", ST_AsText(h.the_geom) as " + geometryPlainTextField + ", ST_distance(w.the_geom,h.the_geom) as " + distField + " from " + aggregated_MeasurementsTableName + " h, "
			+ "(select * from " + original_MeasurementsTableName + " where " + idField + "='"
			+ id_exp
			+ "') w "
			+ "where ST_DWithin(w." + geometryEncodedField + ",h." + geometryEncodedField + ","
			+ distance_exp + ") " + "order by " + distField + " ASC;";
	
	public String pgNearestNeighborCreationString2 = "select h." + idField + ", h." + speedField + ", h." + co2Field + ", h." + generalNumberOfContributingPointsField + ", h." + speedNumberOfContributingPointsField + ", h." + co2NumberOfContributingPointsField + ", h." + generalnumberOfContributingTracksField + ", h." + lastContributingTrackField + ", ST_AsText(h.the_geom) as " + geometryPlainTextField + ", ST_distance(" + geomFromText_exp + ",h.the_geom) as " + distField + " from " + aggregated_MeasurementsTableName + " h, "
			+ "where ST_DWithin(" + geomFromText_exp + ",h." + geometryEncodedField + ","
			+ distance_exp + ") " + "order by " + distField + " ASC;";

	public final String addGeometryColumnToAggregated_MeasurementsTableString = "SELECT AddGeometryColumn( '"
			+ table_name_exp
			+ "', '" + geometryEncodedField + "', " + spatial_ref_sys + ", 'POINT', 2 );";
	
	public final String selectAllMeasurementsofTrackQueryString = "select h." + idField + ", h." + speedField + ", h." + co2Field + ", h." + trackIDField + ", ST_AsText(h.the_geom) as " + geometryPlainTextField + " from " + original_MeasurementsTableName + " h where h." + trackIDField + " = ";
	
	public final String selectAllAggregatedMeasurementsString = "select h." + idField + ", h." + speedField + ", h." + co2Field + ", h." + generalNumberOfContributingPointsField + ", h." + speedNumberOfContributingPointsField + ", h." + co2NumberOfContributingPointsField + ", h." + generalnumberOfContributingTracksField + ", h." + lastContributingTrackField + ", ST_AsText(h.the_geom) as " + geometryPlainTextField + " from " + aggregated_MeasurementsTableName + " h; ";
	
	public final String deletePointFromAggregatedMeasurementsString = "delete from " + aggregated_MeasurementsTableName + " where " + idField + "=";
	
	public PostgresPointService() {
		this(null);
	}
	
	public PostgresPointService(AggregationAlgorithm algorithm) {
		this.algorithm = algorithm;
		trackDatabasePointsResultSetMap = new HashMap<>();
		trackMeasurementsAsPointsMap = new HashMap<>();
		
		createConnection();
		createTable(pgOriginalMeasurementsTableCreationString, original_MeasurementsTableName);
		createTable(pgCreationString, aggregated_MeasurementsTableName);
	}

	@Override
	public Point getNextPoint(String trackID) {
		
		Iterator<Point> measurementIterator =  trackMeasurementsAsPointsMap.get(trackID).iterator();

		if(!measurementIterator.hasNext()){
			return null;
		}		
			
		Point result = measurementIterator.next();
			
		measurementIterator.remove();
			
		return result;
	}

	@Override
	public void getMeasurementsOfTracks(List<String> trackIDs) {
		
		for (String trackID : trackIDs) {
			
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
					}
				}
				
				createPointsFromJSON(features, trackID, algorithm != null ? algorithm.getBbox() : null);
				
			} catch (MalformedURLException e) {
				LOGGER.error("Malformed URL: " + url == null ? null : url.toString(), e);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}

	@Override
	public void getMeasurementsOfTrack(String trackID) {
		
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
			
			createPointsFromJSON(features, trackID, algorithm != null ? algorithm.getBbox() : null);
			
		} catch (MalformedURLException e) {
			LOGGER.error("Malformed URL: " + url == null ? null : url.toString(), e);
		} catch (IOException e) {
			LOGGER.error("Error getting measurements of track:" + trackID, e);
		}
		
	}

	@Override
	public Point getNearestNeighbor(String pointID, double distance) {

		String queryString = pgNearestNeighborCreationString.replace(id_exp,
				pointID).replace(distance_exp, "" + distance);

		ResultSet resultSet = executeQueryStatement(queryString);

		try {

			if (resultSet != null) {

				while (resultSet.next()) {

					String resultID = resultSet.getString(idField);

					Map<String, Object> propertyMap = new HashMap<>();
					
					Map<String, Integer> propertyPointsUsedForAggregationMap = new HashMap<>();

					for (String propertyName : Properties
							.getPropertiesOfInterestDatatypeMapping().keySet()) {

						Class<?> propertyClass = Properties
								.getPropertiesOfInterestDatatypeMapping().get(
										propertyName);

						Object value = null;

						if (propertyClass.equals(Double.class)) {
							value = resultSet.getDouble(propertyName
									.toLowerCase());
						}

						propertyMap.put(propertyName, value);
						
						int pointsUsedForAggregation = resultSet.getInt(propertyName
									.toLowerCase() + "numberofcontributingpoints");
								
						propertyPointsUsedForAggregationMap.put(propertyName, pointsUsedForAggregation);
						
					}

					String resultLastContributingTrack = resultSet
							.getString(lastContributingTrackField);

					int resultNumberOfContributingPoints = resultSet
							.getInt(generalNumberOfContributingPointsField);

					int resultNumberOfContributingTracks = resultSet
							.getInt(generalnumberOfContributingTracksField);

					String resultGeomAsText = resultSet.getString(geometryPlainTextField);

					double[] resultXY = Utils
							.convertWKTPointToXY(resultGeomAsText);

//					LOGGER.debug(resultID);// TODO remove
//					LOGGER.debug("" + resultSet.getDouble(distField));// TODO
//																	// remove
//					LOGGER.debug(resultSet.getString(geometryPlainTextField));// TODO
//																	// remove

					Point resultPoint = new InMemoryPoint(resultID,
							resultXY[0], resultXY[1], propertyMap,
							resultNumberOfContributingPoints,
							resultNumberOfContributingTracks,
							resultLastContributingTrack, propertyPointsUsedForAggregationMap);

					return resultPoint;
				}

			}
		} catch (SQLException e) {
			LOGGER.error("Could not query nearest neighbor of " + pointID, e);
		}

		return null;
	}
	
	@Override
	public boolean updateResultSet(String idOfPointToBeReplaced,
			Point newPoint) {
		
		removePoint(idOfPointToBeReplaced);
		
		addToResultSet(newPoint);
		
		return true;
	}

	@Override
	public List<Point> getResultSet() {
		    	
		List<Point> result = new ArrayList<>();
		
    	ResultSet resultSet = executeQueryStatement(selectAllAggregatedMeasurementsString);
		
    	try {

			if (resultSet != null) {

				while (resultSet.next()) {
					
					Point resultPoint = createPointFromCurrentResultSetEntry(resultSet);
					
					result.add(resultPoint);
				}

			}
		} catch (SQLException e) {
			LOGGER.error("Could not query resultset.", e);
		}
    	
		return result;
	}

	@Override
	public Point aggregate(Point point, Point nearestNeighborPoint) {
		
		Point aggregatedPoint = new InMemoryPoint(point);
		
		updateValues(aggregatedPoint, nearestNeighborPoint);
		
		aggregatedPoint.setID(new ObjectId().toString());
		
//		double averagedX = (aggregatedPoint.getX() + nearestNeighborPoint.getX()) / 2;
//		double averagedY = (aggregatedPoint.getY() + nearestNeighborPoint.getY()) / 2;
		
		/*
		 * distance weighting
		 */
		double averagedX = ((nearestNeighborPoint.getX() * nearestNeighborPoint.getNumberOfPointsUsedForAggregation()) + aggregatedPoint.getX()) / (nearestNeighborPoint.getNumberOfPointsUsedForAggregation() + 1);
		double averagedY = ((nearestNeighborPoint.getY() * nearestNeighborPoint.getNumberOfPointsUsedForAggregation()) + aggregatedPoint.getY()) / (nearestNeighborPoint.getNumberOfPointsUsedForAggregation() + 1);
				
		aggregatedPoint.setNumberOfPointsUsedForAggregation(nearestNeighborPoint.getNumberOfPointsUsedForAggregation()+1);
		
		aggregatedPoint.setX(averagedX);
		aggregatedPoint.setY(averagedY);
		
		LOGGER.debug(aggregatedPoint.getLastContributingTrack() + " vs. " + nearestNeighborPoint.getLastContributingTrack());
		
		if(!aggregatedPoint.getLastContributingTrack().equals(nearestNeighborPoint.getLastContributingTrack())){
			aggregatedPoint.setNumberOfTracksUsedForAggregation(nearestNeighborPoint.getNumberOfTracksUsedForAggregation() +1);
		}else{
			aggregatedPoint.setNumberOfTracksUsedForAggregation(nearestNeighborPoint.getNumberOfTracksUsedForAggregation());
		}
		
		LOGGER.debug("Aggregated: " + aggregatedPoint.getID() + " and " + nearestNeighborPoint.getID());
		LOGGER.debug("NumberOfPoints " + aggregatedPoint.getNumberOfPointsUsedForAggregation());
		
		return aggregatedPoint;
	}

	@Override
	public boolean isFitForAggregation(Point point) {

		boolean result = false;
		
		for (String propertyName : Properties.getPropertiesOfInterestDatatypeMapping().keySet()) {
			
			Object numberObject = point.getProperty(propertyName);
			
			if(numberObject instanceof Number){
				result = result || !isNumberObjectNullOrZero((Number) numberObject);
			}else{
				/*
				 * not a number, we cannot aggregate this currently
				 */
				result = result || false;
			}
		}
		
		return result;
	}

	@Override
	public void addToResultSet(Point newPoint) {		
//		insertPoint(newPoint.getID(), newPoint.getX(), newPoint.getY(), newPoint.getLastContributingTrack(), newPoint.getNumberOfPointsUsedForAggregation(), newPoint.getNumberOfTracksUsedForAggregation(), newPoint.getPropertyMap(), true);
		insertPoint(newPoint, true);
	}
	
	private boolean isNumberObjectNullOrZero(Number numberObject) {

		Number sourceValue = (Number) numberObject;

		double value = sourceValue.doubleValue();

		if (value == 0.0) {
			return true;
		}

		return false;
	}
	
	private boolean removePoint(String pointID){
		return executeUpdateStatement(deletePointFromAggregatedMeasurementsString.concat("'" + pointID + "';"));
	}
	
	private String getDatabaseName() {
		
		if(databaseName == null || databaseName.equals("")){			
			this.databaseName = Properties.getProperty("databaseName").toString();
		}
		
		return databaseName;
	}

	private String getDatabasePath() {
		
		if(databasePath == null || databasePath.equals("")){			
			databasePath = Properties.getProperty("databasePath").toString();
		}
		
		return databasePath;
	}
	
	private String getDatabaseUsername() {
		
		if(username == null || username.equals("")){			
			username = Properties.getProperty("username").toString();
		}
		
		return username;
	}
	
	private String getDatabasePassword() {
		
		if(password == null || password.equals("")){
			this.password = Properties.getProperty("password").toString();
		}
		
		return password;
	}
	
	private void updateValues(Point source, Point closestPointInRange){
		
		for (String propertyName : Properties.getPropertiesOfInterestDatatypeMapping().keySet()) {
			
			double weightedAvg = getWeightedAverage(source, closestPointInRange, propertyName);
			
			LOGGER.debug("Average: " + weightedAvg);
			
			source.setProperty(propertyName, weightedAvg);
		}
		
	}
	
	private double getWeightedAverage(Point source, Point closestPointInRange,
			String propertyName) {

		Object sourceNumberObject = source.getProperty(propertyName);

		if (sourceNumberObject instanceof Number) {

			Number sourceValue = (Number) sourceNumberObject;

			double summedUpValues = sourceValue.doubleValue();

			LOGGER.debug("Value of " + propertyName + " of point " + source.getID() + " = " + summedUpValues);
			
			Object pointNumberObject = closestPointInRange.getProperty(propertyName);
			if (pointNumberObject instanceof Number) {
				
				double d = ((Number) pointNumberObject).doubleValue();
				
				int numberOfPointsUsedForAggregation = closestPointInRange.getNumberOfPointsUsedForAggregation(propertyName);
				
				/*
				 * sort out values of 0.0
				 */
				if(d == 0.0){
					LOGGER.debug("Value for aggregation is 0.0, will not use this.");
					source.setNumberOfPointsUsedForAggregation(numberOfPointsUsedForAggregation, propertyName);
					return summedUpValues;
				}
				
				double weightedAvg = ((d * numberOfPointsUsedForAggregation) + summedUpValues) / (numberOfPointsUsedForAggregation + 1);
				
				source.setNumberOfPointsUsedForAggregation(numberOfPointsUsedForAggregation + 1, propertyName);
				
				LOGGER.debug("Value of " + propertyName + " of point " + closestPointInRange.getID() + " = " + d);

				return weightedAvg;
			}
		}

		LOGGER.debug("Source property " + propertyName + " is not a number.");
		
		return -1;
	}
	
	private Point createPointFromCurrentResultSetEntry(ResultSet resultSet) throws SQLException{
		
		String resultID = resultSet.getString(idField);

		Map<String, Object> propertyMap = new HashMap<>();
		
		Map<String, Integer> propertyPointsUsedForAggregationMap = new HashMap<>();

		for (String propertyName : Properties
				.getPropertiesOfInterestDatatypeMapping().keySet()) {

			Class<?> propertyClass = Properties
					.getPropertiesOfInterestDatatypeMapping().get(
							propertyName);

			Object value = null;

			if (propertyClass.equals(Double.class)) {
				value = resultSet.getDouble(propertyName
						.toLowerCase());
			}

			propertyMap.put(propertyName, value);
			
			try{
			
			int pointsUsedForAggregation = resultSet.getInt(propertyName
						.toLowerCase() + "numberofcontributingpoints");
					
			propertyPointsUsedForAggregationMap.put(propertyName, pointsUsedForAggregation);
			}catch(Exception e){
				LOGGER.info("Column " + propertyName.toLowerCase() + "numberofcontributingpoints" + " not available.");
				LOGGER.info(e.getMessage());
			}
			
		}
		
		String resultLastContributingTrack = "";
		
		try {
			resultLastContributingTrack = resultSet.getString(lastContributingTrackField);			
		} catch (SQLException e) {
			LOGGER.info("Column " + lastContributingTrackField + " not available.");
			LOGGER.info(e.getMessage());
		}
		
		int resultNumberOfContributingPoints = 1;
		
		try {			
			resultNumberOfContributingPoints = resultSet.getInt(generalNumberOfContributingPointsField);
		} catch (SQLException e) {
			LOGGER.info("Column " + generalNumberOfContributingPointsField + " not available. Defaulting to 1.");
			LOGGER.info(e.getMessage());
		}
		
		int resultNumberOfContributingTracks = 1;
		
		try {			
			resultNumberOfContributingTracks = resultSet.getInt(generalnumberOfContributingTracksField);			
		} catch (SQLException e) {
			LOGGER.info("Column " + generalnumberOfContributingTracksField + " not available. Defaulting to 1.");
			LOGGER.info(e.getMessage());
		}
		
		if(resultLastContributingTrack == null || resultLastContributingTrack.isEmpty()){
			/*
			 * point seems to be original point, try accessing trackid column 
			 */
			try {
				resultLastContributingTrack = resultSet.getString(trackIDField);			
			} catch (SQLException e) {
				LOGGER.info("Column " + trackIDField + " not available.");
				LOGGER.info(e.getMessage());
			}
		}
		
		double[] resultXY = new double[2];
		
		try {
			
			String resultGeomAsText = resultSet.getString(geometryPlainTextField);
			
			resultXY = Utils.convertWKTPointToXY(resultGeomAsText);
			
		} catch (SQLException e) {
			LOGGER.info("Column " + geometryPlainTextField + " not available.");
			LOGGER.error(e.getMessage());
		}
		
		Point resultPoint = new InMemoryPoint(resultID, resultXY[0], resultXY[1], propertyMap, resultNumberOfContributingPoints, resultNumberOfContributingTracks, resultLastContributingTrack, propertyPointsUsedForAggregationMap);
		
		return resultPoint;
		
	}
	
    private void createPointsFromJSON(ArrayList<?> features, String trackID, Geometry bbox) {
    	
    	List<Point> measurementsOfTrack = new ArrayList<>();
    	
    	for (Object object : features) {

			if (object instanceof LinkedHashMap<?, ?>) {
				LinkedHashMap<?, ?> featureMap = (LinkedHashMap<?, ?>) object;

				Object geometryObject = featureMap.get("geometry");
				
				double[] coordinatesXY = new double[2];
				
				if (geometryObject instanceof LinkedHashMap<?, ?>) {
					coordinatesXY = Utils.getCoordinatesXYFromJSON((LinkedHashMap<?, ?>) geometryObject);
				}
				
				Coordinate pointCoordinate = new Coordinate(coordinatesXY[0], coordinatesXY[1]);
				
				if (bbox != null) {
					if (!bbox.contains(Utils.geometryFactory
							.createPoint(pointCoordinate))) {
						continue;
					}
				}
				Object propertiesObject = featureMap.get("properties");				
				
				if (propertiesObject instanceof LinkedHashMap<?, ?>) {
					LinkedHashMap<?, ?> propertiesMap = (LinkedHashMap<?, ?>) propertiesObject;

					String id = String.valueOf(propertiesMap.get("id"));
					
					Object phenomenonsObject = propertiesMap.get("phenomenons");

					if (phenomenonsObject instanceof LinkedHashMap<?, ?>) {
						LinkedHashMap<?, ?> phenomenonsMap = (LinkedHashMap<?, ?>) phenomenonsObject;

						Map<String, Object> propertiesofInterestMap = Utils.getValuesFromFromJSON(phenomenonsMap);
						
						Point point = new InMemoryPoint(id, coordinatesXY[0], coordinatesXY[1], propertiesofInterestMap, 0, 0, trackID, new HashMap<String, Integer>());
						
						measurementsOfTrack.add(point);
						
					}
				}
			}
		}
    	
    	trackMeasurementsAsPointsMap.put(trackID, measurementsOfTrack);
    	
	}
	
	private boolean createConnection() {

		connectionURL = "jdbc:postgresql:" + getDatabasePath() + "/"
				+ getDatabaseName();

		java.util.Properties props = new java.util.Properties();

		props.setProperty("create", "true");
		props.setProperty("user", getDatabaseUsername());
		props.setProperty("password", getDatabasePassword());
		conn = null;
		try {
			conn = DriverManager.getConnection(connectionURL, props);
			conn.setAutoCommit(false);
			LOGGER.info("Connected to measurement database.");
		} catch (SQLException e) {
			LOGGER.error("Could not connect to or create the database.", e);
			return false;
		}

		return true;
	}

	private boolean createTable(String creationString, String tableName) {
		try {
			ResultSet rs;
			DatabaseMetaData meta = conn.getMetaData();
			rs = meta
					.getTables(null, null, tableName, new String[] { "TABLE" });
			if (!rs.next()) {
				LOGGER.info("Table " + tableName + " does not yet exist.");
				Statement st = conn.createStatement();
				st.executeUpdate(creationString);

				conn.commit();

				meta = conn.getMetaData();

				rs = meta.getTables(null, null, tableName,
						new String[] { "TABLE" });
				if (rs.next()) {
					LOGGER.info("Succesfully created table " + tableName + ".");
					
					/*
					 * add geometry column
					 */
					executeStatement(addGeometryColumnToAggregated_MeasurementsTableString.replace(table_name_exp, tableName));
					
				} else {
					LOGGER.error("Could not create table " + tableName + ".");
					return false;
				}
			}
		} catch (SQLException e) {
			LOGGER.error("Connection to the Postgres database failed: "
					+ e.getMessage());
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
			LOGGER.error("Execution of the following statement failed: "
					+ statement + " cause: " + e.getMessage());
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
			LOGGER.error("Execution of the following statement failed: "
					+ statement + " cause: " + e.getMessage());
			return null;
		}
	}

	private boolean executeUpdateStatement(String statement) {
		try {
			Statement st = conn.createStatement();
			st.executeUpdate(statement);

			conn.commit();

		} catch (SQLException e) {
			LOGGER.error("Execution of the following statement failed: "
					+ statement + " cause: " + e.getMessage());
			return false;
		}
		return true;
	}

	private boolean insertPoint(String id, double x, double y, String trackID,
			int numberOfPoints, int numberOfTracks, Map<String, Object> propertiesofInterestMap, boolean checkIfExists) {

		if(checkIfExists){
			String statement = "select * from " + aggregated_MeasurementsTableName + " where "+ idField +"='" + id + "';";
			
			ResultSet rs = executeQueryStatement(statement);
			
			try {
				if(rs.next()){
					return false;
				}
			} catch (SQLException e) {
				LOGGER.error("Could not check if row with id=" + id + " and "+ trackIDField +"=" + trackID + " exists.", e);
				return false;
			}
		}
		
		String statement = createInsertPointStatement(id, x, y, trackID,
				numberOfPoints, numberOfTracks, propertiesofInterestMap);

		return executeUpdateStatement(statement);
	}

	private String createInsertPointStatement(String id, double x, double y,
			String trackID, int numberOfPoints, int numberOfTracks,
			Map<String, Object> propertiesofInterestMap) {

		String columnNameString = "( "+ idField +", "+ geometryEncodedField +", "+ generalNumberOfContributingPointsField +", "+ generalnumberOfContributingTracksField +", "+ lastContributingTrackField +", ";
		String valueString = "( '" + id + "', ST_GeomFromText('POINT(" + x
				+ " " + y + ")', " + spatial_ref_sys + "), " + numberOfPoints
				+ ", " + numberOfTracks + ", '" + trackID + "', ";

		Iterator<String> propertyNameIterator = Properties
				.getPropertiesOfInterestDatatypeMapping().keySet().iterator();

		while (propertyNameIterator.hasNext()) {
			String propertyName = (String) propertyNameIterator.next();

			columnNameString = columnNameString.concat(propertyName
					.toLowerCase());
			valueString = valueString.concat(String
					.valueOf(propertiesofInterestMap.get(propertyName)));

			if (propertyNameIterator.hasNext()) {
				columnNameString = columnNameString.concat(", ");
				valueString = valueString.concat(", ");
			} else {
				columnNameString = columnNameString.concat(")");
				valueString = valueString.concat(")");
			}
		}

		String statement = "INSERT INTO " + aggregated_MeasurementsTableName
				+ columnNameString + "VALUES" + valueString + ";";

		return statement;
	}

	private boolean insertPoint(Point point,  boolean checkIfExists) {

		if(checkIfExists){
			String statement = "select * from " + aggregated_MeasurementsTableName + " where "+ idField +"='" + point.getID() + "';";
			
			ResultSet rs = executeQueryStatement(statement);
			
			try {
				if(rs.next()){
					return false;
				}
			} catch (SQLException e) {
				LOGGER.error("Could not check if row with id=" + point.getID() + " and "+ trackIDField +"=" + point.getLastContributingTrack() + " exists.", e);
				return false;
			}
		}
		
		String statement = createInsertPointStatement(point);

		return executeUpdateStatement(statement);
	}
	
	private String createInsertPointStatement(Point point) {

		String columnNameString = "( "+ idField +", "+ geometryEncodedField +", "+ generalNumberOfContributingPointsField +", "+ generalnumberOfContributingTracksField +", "+ lastContributingTrackField +", ";
		String valueString = "( '" + point.getID() + "', ST_GeomFromText('POINT(" + point.getX()
				+ " " + point.getY() + ")', " + spatial_ref_sys + "), " + point.getNumberOfPointsUsedForAggregation()
				+ ", " + point.getNumberOfTracksUsedForAggregation() + ", '" + point.getLastContributingTrack() + "', ";

		Iterator<String> propertyNameIterator = Properties
				.getPropertiesOfInterestDatatypeMapping().keySet().iterator();

		while (propertyNameIterator.hasNext()) {
			String propertyName = (String) propertyNameIterator.next();

			columnNameString = columnNameString.concat(propertyName
					.toLowerCase());
			
			columnNameString = columnNameString.concat(", ");
			
			columnNameString = columnNameString.concat(propertyName
					.toLowerCase() + "numberofcontributingpoints");
			
			Object o = point.getProperty(propertyName);
			
			valueString = valueString.concat(o == null? "" : String
					.valueOf(o));
			
			valueString = valueString.concat(", ");
			
			o = point.getNumberOfPointsUsedForAggregation(propertyName);
			
			valueString = valueString.concat(String
					.valueOf(o == null? 0 : String
							.valueOf(o)));

			if (propertyNameIterator.hasNext()) {
				columnNameString = columnNameString.concat(", ");
				valueString = valueString.concat(", ");
			} else {
				columnNameString = columnNameString.concat(")");
				valueString = valueString.concat(")");
			}
		}

		String statement = "INSERT INTO " + aggregated_MeasurementsTableName
				+ columnNameString + "VALUES" + valueString + ";";

		return statement;
	}
	
	private boolean insertPoint(String id, double x, double y, String trackID, Map<String, Object> propertiesofInterestMap, boolean checkIfExists) {

		if(checkIfExists){
			String statement = "select * from "+ original_MeasurementsTableName +" where "+ idField +"='" + id + "' and "+ trackIDField +"='" + trackID + "';";
			
			ResultSet rs = executeQueryStatement(statement);
			
			try {
				if(rs.next()){
					return false;
				}
			} catch (SQLException e) {
				LOGGER.error("Could not check if row with id=" + id + " and "+ trackIDField +"=" + trackID + " exists.", e);
				return false;
			}
		}
		
		String statement = createInsertPointStatement(id, x, y, trackID,
				propertiesofInterestMap);

		return executeUpdateStatement(statement);
	}
	
	private String createInsertPointStatement(String id, double x, double y,
			String trackID,	Map<String, Object> propertiesofInterestMap) {

		String columnNameString = "( "+ idField +", "+ geometryEncodedField +", "+ trackIDField +", ";
		String valueString = "( '" + id + "', ST_GeomFromText('POINT(" + x
				+ " " + y + ")', " + spatial_ref_sys + "), '" + trackID + "', ";

		Iterator<String> propertyNameIterator = Properties
				.getPropertiesOfInterestDatatypeMapping().keySet().iterator();

		while (propertyNameIterator.hasNext()) {
			String propertyName = (String) propertyNameIterator.next();

			columnNameString = columnNameString.concat(propertyName
					.toLowerCase());
			valueString = valueString.concat(String
					.valueOf(propertiesofInterestMap.get(propertyName)));

			if (propertyNameIterator.hasNext()) {
				columnNameString = columnNameString.concat(", ");
				valueString = valueString.concat(", ");
			} else {
				columnNameString = columnNameString.concat(")");
				valueString = valueString.concat(")");
			}
		}

		String statement = "INSERT INTO " + original_MeasurementsTableName
				+ columnNameString + "VALUES" + valueString + ";";

		return statement;
	}
	
	
}
